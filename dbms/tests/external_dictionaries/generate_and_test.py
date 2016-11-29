import os
import json
import subprocess
import time
from itertools import chain
from os import system

wait_for_loading_sleep_time_sec = 3
continue_on_error = False

clickhouse_binary = 'clickhouse-client'
prefix = base_dir = os.path.dirname(os.path.realpath(__file__))
generated_prefix = prefix + '/generated/'

# [ name, key_type, has_parent ]
dictionaries = [
    # Simple key dictionaries
    [ 'file_flat', 0, True ],
    [ 'clickhouse_flat', 0, True ],
    [ 'mysql_flat', 0, True ],
    [ 'mongodb_flat', 0, True ],

    [ 'file_hashed', 0, True ],
    [ 'clickhouse_hashed', 0, True ],
    [ 'mysql_hashed', 0, True ],
    [ 'mongodb_hashed', 0, True ],

    [ 'clickhouse_cache', 0, True ],
    [ 'mysql_cache', 0, True ],
    [ 'mongodb_cache', 0, True ],

    # Complex key dictionaries with (UInt8, UInt8) key
    [ 'file_complex_integers_key_hashed', 1, False ],
    [ 'clickhouse_complex_integers_key_hashed', 1, False ],
    [ 'mysql_complex_integers_key_hashed', 1, False ],
    [ 'mongodb_complex_integers_key_hashed', 1, False ],

    [ 'clickhouse_complex_integers_key_cache', 1, False ],
    [ 'mysql_complex_integers_key_cache', 1, False ],
    [ 'mongodb_complex_integers_key_cache', 1, False ],

    # Complex key dictionaries with (String, UInt8) key
    [ 'file_complex_mixed_key_hashed', 2, False ],
    [ 'clickhouse_complex_mixed_key_hashed', 2, False ],
    [ 'mysql_complex_mixed_key_hashed', 2, False ],
    [ 'mongodb_complex_mixed_key_hashed', 2, False ],

    [ 'clickhouse_complex_mixed_key_cache', 2, False ],
    [ 'mysql_complex_mixed_key_cache', 2, False ],
    [ 'mongodb_complex_mixed_key_cache', 2, False ],
]

files = [ 'key_simple.tsv', 'key_complex_integers.tsv', 'key_complex_mixed.tsv' ]

types = [
    'UInt8', 'UInt16', 'UInt32', 'UInt64',
    'Int8', 'Int16', 'Int32', 'Int64',
    'Float32', 'Float64',
    'String',
    'Date', 'DateTime'
]

explicit_defaults = [
    '42', '42', '42', '42',
    '-42', '-42', '-42', '-42',
    '1.5', '1.6',
    "'explicit-default'",
    "'2015-01-01'", "'2015-01-01 00:00:00'"
]

implicit_defaults = [
    '1', '1', '1', '1',
    '-1', '-1', '-1', '-1',
    '2.71828', '2.71828',
    'implicit-default',
    '2015-11-25', '2015-11-25 00:00:00'
]

def call(args, out_filename):
    with open(out_filename, 'w') as file:
        subprocess.check_call(args, stdout=file)

def generate_data():
    def comma_separated(iterable):
        return ', '.join(iterable)

    def columns():
        return map(lambda t: t + '_', types)

    key_columns = [
        [ 'id' ],
        [ 'key0', 'key1' ],
        [ 'key0_str', 'key1' ]
    ]

    print 'Creating ClickHouse table'
    # create ClickHouse table via insert select
    system('cat source.tsv | {ch} --port 9001 -m -n --query "'
              'create database if not exists test;'
              'drop table if exists test.dictionary_source;'
              'create table test.dictionary_source ('
                    'id UInt64, key0 UInt8, key0_str String, key1 UInt8,'
                    'UInt8_ UInt8, UInt16_ UInt16, UInt32_ UInt32, UInt64_ UInt64,'
                    'Int8_ Int8, Int16_ Int16, Int32_ Int32, Int64_ Int64,'
                    'Float32_ Float32, Float64_ Float64,'
                    'String_ String,'
                    'Date_ Date, DateTime_ DateTime, Parent UInt64'
              ') engine=Log; insert into test.dictionary_source format TabSeparated'
              '"'.format(ch=clickhouse_binary))

    # generate 3 files with different key types
    print 'Creating .tsv files'
    file_source_query = 'select %s from test.dictionary_source format TabSeparated;'
    for file, keys in zip(files, key_columns):
        query = file_source_query % comma_separated(chain(keys, columns(), [ 'Parent' ] if 1 == len(keys) else []))
        call([ clickhouse_binary, '--port', '9001', '--query', query ], 'generated/' + file)

    # create MySQL table from complete_query
    print 'Creating MySQL table'
    subprocess.check_call('echo "'
              'create database if not exists test;'
              'drop table if exists test.dictionary_source;'
              'create table test.dictionary_source ('
                    'id tinyint unsigned, key0 tinyint unsigned, key0_str text, key1 tinyint unsigned, '
                    'UInt8_ tinyint unsigned, UInt16_ smallint unsigned, UInt32_ int unsigned, UInt64_ bigint unsigned, '
                    'Int8_ tinyint, Int16_ smallint, Int32_ int, Int64_ bigint, '
                    'Float32_ float, Float64_ double, '
                    'String_ text, Date_ date, DateTime_ datetime, Parent bigint unsigned'
              ');'
              'load data local infile \'{0}/source.tsv\' into table test.dictionary_source;" | mysql --local-infile=1'
              .format(prefix), shell=True)

    # create MongoDB collection from complete_query via JSON file
    print 'Creating MongoDB collection'
    table_rows = json.loads(subprocess.check_output([
        clickhouse_binary, '--port', '9001',
        '--query',
        "select * from test.dictionary_source where not ignore(" \
            "concat('new Date(\\'', toString(Date_), '\\')') as Date_, " \
            "concat('new Date(\\'', toString(DateTime_), '\\')') as DateTime_" \
        ") format JSON"
    ]))['data']

    # ClickHouse outputs 64-bit wide integers in double-quotes, convert them
    for row in table_rows:
        for column in [u'id', u'UInt64_', u'Int64_', u'Parent']:
            row[column] = int(row[column])

    source_for_mongo = json.dumps(table_rows).replace(')"', ')').replace('"new', 'new')
    open('generated/full.json', 'w').write('db.dictionary_source.drop(); db.dictionary_source.insert(%s);' % source_for_mongo)
    result = system('cat generated/full.json | mongo --quiet > /dev/null')
    if result != 0:
        print 'Could not create MongoDB collection'
        exit(-1)

def generate_dictionaries():
    dictionary_skeleton = '''
    <dictionaries>
        <dictionary>
            <name>{name}</name>

            <source>
                {source}
            </source>

            <lifetime>
                <min>0</min>
                <max>0</max>
            </lifetime>

            <layout>
                {layout}
            </layout>

            <structure>
                {key}

                %s

                {parent}
            </structure>
        </dictionary>
    </dictionaries>'''
    attribute_skeleton = '''
    <attribute>
        <name>%s_</name>
        <type>%s</type>
        <null_value>%s</null_value>
    </attribute>
    '''

    dictionary_skeleton =\
        dictionary_skeleton % reduce(lambda xml, (type, default): xml + attribute_skeleton % (type, type, default),
                                     zip(types, implicit_defaults), '')

    source_file = '''
    <file>
        <path>%s</path>
        <format>TabSeparated</format>
    </file>
    '''
    source_clickhouse = '''
    <clickhouse>
        <host>127.0.0.1</host>
        <port>9001</port>
        <user>default</user>
        <password></password>
        <db>test</db>
        <table>dictionary_source</table>
    </clickhouse>
    '''
    source_mysql = '''
    <mysql>
        <host>127.0.0.1</host>
        <port>3306</port>
        <user>root</user>
        <password></password>
        <db>test</db>
        <table>dictionary_source</table>
    </mysql>
    '''
    source_mongodb = '''
    <mongodb>
        <host>localhost</host>
        <port>27017</port>
        <user></user>
        <password></password>
        <db>test</db>
        <collection>dictionary_source</collection>
    </mongodb>
    '''

    layout_flat = '<flat />'
    layout_hashed = '<hashed />'
    layout_cache = '<cache><size_in_cells>128</size_in_cells></cache>'
    layout_complex_key_hashed = '<complex_key_hashed />'
    layout_complex_key_cache = '<complex_key_cache><size_in_cells>128</size_in_cells></complex_key_cache>'

    key_simple = '''
    <id>
        <name>id</name>
    </id>
    '''
    key_complex_integers = '''
    <key>
        <attribute>
            <name>key0</name>
            <type>UInt8</type>
        </attribute>

        <attribute>
            <name>key1</name>
            <type>UInt8</type>
        </attribute>
    </key>
    '''
    key_complex_mixed = '''
    <key>
        <attribute>
            <name>key0_str</name>
            <type>String</type>
        </attribute>

        <attribute>
            <name>key1</name>
            <type>UInt8</type>
        </attribute>
    </key>
    '''

    keys = [ key_simple, key_complex_integers, key_complex_mixed ]

    parent_attribute = '''
    <attribute>
        <name>Parent</name>
        <type>UInt64</type>
        <hierarchical>true</hierarchical>
        <null_value>0</null_value>
    </attribute>
    '''

    sources_and_layouts = [
        # Simple key dictionaries
        [ source_file % (generated_prefix + files[0]), layout_flat],
        [ source_clickhouse, layout_flat ],
        [ source_mysql, layout_flat ],
        [ source_mongodb, layout_flat ],

        [ source_file % (generated_prefix + files[0]), layout_hashed],
        [ source_clickhouse, layout_hashed ],
        [ source_mysql, layout_hashed ],
        [ source_mongodb, layout_hashed ],

        [ source_clickhouse, layout_cache ],
        [ source_mysql, layout_cache ],
        [ source_mongodb, layout_cache ],

        # Complex key dictionaries with (UInt8, UInt8) key
        [ source_file % (generated_prefix + files[1]), layout_complex_key_hashed],
        [ source_clickhouse, layout_complex_key_hashed ],
        [ source_mysql, layout_complex_key_hashed ],
        [ source_mongodb, layout_complex_key_hashed ],

        [ source_clickhouse, layout_complex_key_cache ],
        [ source_mysql, layout_complex_key_cache ],
        [ source_mongodb, layout_complex_key_cache ],

        # Complex key dictionaries with (String, UInt8) key
        [ source_file % (generated_prefix + files[2]), layout_complex_key_hashed],
        [ source_clickhouse, layout_complex_key_hashed ],
        [ source_mysql, layout_complex_key_hashed ],
        [ source_mongodb, layout_complex_key_hashed ],

        [ source_clickhouse, layout_complex_key_cache ],
        [ source_mysql, layout_complex_key_cache ],
        [ source_mongodb, layout_complex_key_cache ],
    ]

    for (name, key_idx, has_parent), (source, layout) in zip(dictionaries, sources_and_layouts):
        filename = 'generated/dictionary_%s.xml' % name
        with open(filename, 'w') as file:
            dictionary_xml = dictionary_skeleton.format(
                key = keys[key_idx], parent = parent_attribute if has_parent else '', **locals())
            file.write(dictionary_xml)

def run_tests():
    keys = [ 'toUInt64(n)', '(n, n)', '(toString(n), n)' ]
    dict_get_query_skeleton = "select dictGet{type}('{name}', '{type}_', {key}) array join range(8) as n;"
    dict_has_query_skeleton = "select dictHas('{name}', {key}) array join range(8) as n;"
    dict_get_or_default_query_skeleton = "select dictGet{type}OrDefault('{name}', '{type}_', {key}, to{type}({default})) array join range(8) as n;"
    dict_hierarchy_query_skeleton = "select dictGetHierarchy('{name}' as d, key), dictIsIn(d, key, toUInt64(1)), dictIsIn(d, key, key) array join range(toUInt64(8)) as key;"

    def test_query(dict, query, reference, name):
        result = system('{ch} --port 9001 --query "{query}" | diff - reference/{reference}.reference'.format(ch=clickhouse_binary, **locals()))

        if result != 0:
            print 'Dictionary ' + dict + ' has failed test ' + name + '\n'
            if not continue_on_error:
                exit(-1)

    print 'Waiting for dictionaries to load...'
    time.sleep(wait_for_loading_sleep_time_sec)

    # the actual tests
    for (name, key_idx, has_parent) in dictionaries:
        key = keys[key_idx]
        print 'Testing dictionary', name

        # query dictHas
        test_query(name, dict_has_query_skeleton.format(**locals()), 'has', 'dictHas')

        # query dictGet*
        for type, default in zip(types, explicit_defaults):
            test_query(name,
                dict_get_query_skeleton.format(**locals()),
                type, 'dictGet' + type)
            test_query(name,
                dict_get_or_default_query_skeleton.format(**locals()),
                type + 'OrDefault', 'dictGet' + type + 'OrDefault')

        # query dictGetHierarchy, dictIsIn
        if has_parent:
            test_query(name,
                dict_hierarchy_query_skeleton.format(**locals()),
                'hierarchy', ' for dictGetHierarchy, dictIsIn')

generate_data()
generate_dictionaries()
run_tests()

print 'Done'