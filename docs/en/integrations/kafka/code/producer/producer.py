import argparse
import json
import sys
from genson import SchemaBuilder
from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic, KafkaError, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer


def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(
        description="Confluent Python Client example to produce messages \
                  to Confluent Cloud, optionally with a schema")
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    required.add_argument('-c',
                          dest="config_file",
                          help="path to Confluent Cloud configuration file",
                          required=True)
    args = parser.parse_args()

    return args


def read_ccloud_config(config_file):
    """Read Confluent Cloud configuration for librdkafka clients"""

    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()

    # conf['ssl.ca.location'] = certifi.where()

    return conf


def pop_conf_value(conf, value):
    if value in conf:
        conf.pop(value)


def pop_non_producer_conf(conf):
    pop_conf_value(conf, 'basic.auth.credentials.source')
    pop_conf_value(conf, 'schema.registry.url')
    pop_conf_value(conf, 'schema.registry.basic.auth.user.info')
    pop_conf_value(conf, 'output.topic')
    pop_conf_value(conf, 'output.num_messages')
    pop_conf_value(conf, 'input.file')
    pop_conf_value(conf, 'input.schema')
    pop_conf_value(conf, 'input.filter_fields')


def create_topic(conf, topic):
    """
        Create a topic if needed
        Examples of additional admin API functionality:
        https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/adminapi.py
    """

    a = AdminClient(conf)

    fs = a.create_topics([NewTopic(
        topic,
        num_partitions=1,
        replication_factor=3
    )])
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            # Continue if error code TOPIC_ALREADY_EXISTS, which may be true
            # Otherwise fail fast
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                print("Failed to create topic {}: {}".format(topic, e))
                sys.exit(1)


def read_file(file, num_messages):
    d = 0
    while d != num_messages:
        with open(file, "r") as data_file:
            for line in data_file:
                doc = json.loads(line)
                d += 1
                yield doc
                if d == num_messages:
                    return
        if num_messages == -1:
            num_messages = d


def filter(doc, fields):
    if len(filter_fields) == 0:
        return doc
    for field in fields:
        if field in doc:
            del doc[field]
    return doc


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = parse_args()
    config_file = args.config_file

    conf = read_ccloud_config(config_file)
    topic = conf['output.topic']
    num_messages = int(conf['output.num_messages'])
    input_file = conf['input.file']
    filter_fields = []
    if 'input.filter_fields' in conf:
        filter_fields = conf['input.filter_fields'].split(',')
    if 'input.schema' in conf:
        schema_file = conf['input.schema']
        with open(schema_file, "r") as schema_file:
            schema = json.load(schema_file)
    else:
        builder = SchemaBuilder()
        builder.add_schema({"type": "object", "properties": {}})
        for doc in read_file(input_file, 100):
            builder.add_object(filter(doc, filter_fields))
        schema = builder.to_schema()
        schema["title"] = f"auto-generated - {topic}"
    print("schema: ")
    print(json.dumps(schema, indent=2))
    schema_registry_conf = {
        'url': conf['schema.registry.url'],
        'basic.auth.user.info': conf['schema.registry.basic.auth.user.info']}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    value_serializer = JSONSerializer(schema_registry_client=schema_registry_client, schema_str=json.dumps(schema))
    pop_non_producer_conf(conf)
    create_topic(conf, topic)
    conf['value.serializer'] = value_serializer
    producer = SerializingProducer(conf)
    delivered_records = 0

    def acked(err, msg):
        global delivered_records
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
        if delivered_records % 1000 == 0:
            print(
                f"Produced {delivered_records} records to topic {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset()}")

    i = 0
    for doc in read_file(input_file, num_messages):
        doc = filter(doc, filter_fields)
        # we don't need a key
        producer.produce(topic, key=None, value=doc, on_delivery=acked)
        producer.poll(0)
    producer.flush()
    print("{} messages were produced to topic {}!".format(delivered_records, topic))
