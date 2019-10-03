DROP TABLE IF EXISTS merge_a;
DROP TABLE IF EXISTS merge_b;
DROP TABLE IF EXISTS merge_ab;
DROP TABLE IF EXISTS kafka;
DROP TABLE IF EXISTS as_kafka;

CREATE TABLE merge_a (x UInt8) ENGINE = StripeLog;
CREATE TABLE merge_b (x UInt8) ENGINE = StripeLog;
CREATE TABLE merge_ab AS merge(currentDatabase(), '^merge_[ab]$');

CREATE TABLE kafka (x UInt8)
    ENGINE = Kafka
    SETTINGS kafka_broker_list = 'kafka',
             kafka_topic_list = 'topic',
             kafka_group_name = 'group',
             kafka_format = 'CSV';
CREATE TABLE as_kafka AS kafka ENGINE = Memory;

SELECT * FROM system.columns WHERE database = currentDatabase() AND table = 'merge_ab';
SELECT * FROM system.columns WHERE database = currentDatabase() AND table = 'as_kafka';

DROP TABLE merge_a;
DROP TABLE merge_b;
DROP TABLE merge_ab;
DROP TABLE kafka;
DROP TABLE as_kafka;
