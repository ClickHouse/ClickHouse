#!/usr/bin/env python
import sys
import os
import os.path
import json
import subprocess
import time
import lxml.etree as et
import atexit
from itertools import chain
from os import system
from argparse import ArgumentParser
from termcolor import colored
from subprocess import check_call
from subprocess import Popen
from subprocess import PIPE
from subprocess import CalledProcessError
from datetime import datetime
from time import sleep
from errno import ESRCH
from pprint import pprint


OP_SQUARE_BRACKET = colored("[", attrs=['bold'])
CL_SQUARE_BRACKET = colored("]", attrs=['bold'])

MSG_FAIL = OP_SQUARE_BRACKET + colored(" FAIL ", "red", attrs=['bold']) + CL_SQUARE_BRACKET
MSG_UNKNOWN = OP_SQUARE_BRACKET + colored(" UNKNOWN ", "yellow", attrs=['bold']) + CL_SQUARE_BRACKET
MSG_OK = OP_SQUARE_BRACKET + colored(" OK ", "green", attrs=['bold']) + CL_SQUARE_BRACKET
MSG_SKIPPED = OP_SQUARE_BRACKET + colored(" SKIPPED ", "cyan", attrs=['bold']) + CL_SQUARE_BRACKET

wait_for_loading_sleep_time_sec = 3

failures = 0
SERVER_DIED = False

prefix = base_dir = os.path.dirname(os.path.realpath(__file__))
generated_prefix = prefix + '/generated/'


dictionaries = []

def generate_structure(args):
    global dictionaries
      # [ name, key_type, has_parent ]
    dictionaries.extend([
        # Simple key dictionaries
        [ 'file_flat', 0, True ],
        [ 'clickhouse_flat', 0, True ],
        [ 'executable_flat', 0, True ],

        [ 'file_hashed', 0, True ],
        [ 'clickhouse_hashed', 0, True ],
        [ 'executable_hashed', 0, True ],

        [ 'clickhouse_cache', 0, True ],
        [ 'executable_cache', 0, True ],

        # Complex key dictionaries with (UInt8, UInt8) key
        [ 'file_complex_integers_key_hashed', 1, False ],
        [ 'clickhouse_complex_integers_key_hashed', 1, False ],
        [ 'executable_complex_integers_key_hashed', 1, False ],

        [ 'clickhouse_complex_integers_key_cache', 1, False ],
        [ 'executable_complex_integers_key_cache', 1, False ],

        # Complex key dictionaries with (String, UInt8) key
        [ 'file_complex_mixed_key_hashed', 2, False ],
        [ 'clickhouse_complex_mixed_key_hashed', 2, False ],
        [ 'executable_complex_mixed_key_hashed', 2, False ],

        [ 'clickhouse_complex_mixed_key_cache', 2, False ],
        [ 'executable_complex_mixed_key_hashed', 2, False ],
    ])

    if not args.no_http:
        dictionaries.extend([
            [ 'http_flat', 0, True ],
            [ 'http_hashed', 0, True ],
            [ 'http_cache', 0, True ],
            [ 'http_complex_integers_key_hashed', 1, False ],
            [ 'http_complex_integers_key_cache', 1, False ],
            [ 'http_complex_mixed_key_hashed', 2, False ],
            [ 'http_complex_mixed_key_hashed', 2, False ],
        ])

    if not args.no_https:
        dictionaries.extend([
            [ 'https_flat', 0, True ],
            [ 'https_hashed', 0, True ],
            [ 'https_cache', 0, True ],
        ])

    if not args.no_mysql:
        dictionaries.extend([
            [ 'mysql_flat', 0, True ],
            [ 'mysql_hashed', 0, True ],
            [ 'mysql_cache', 0, True ],
            [ 'mysql_complex_integers_key_hashed', 1, False ],
            [ 'mysql_complex_integers_key_cache', 1, False ],
            [ 'mysql_complex_mixed_key_hashed', 2, False ],
            [ 'mysql_complex_mixed_key_cache', 2, False ],
        ])

    if not args.no_mongo:
        dictionaries.extend([
            [ 'mongodb_flat', 0, True ],
            [ 'mongodb_hashed', 0, True ],
            [ 'mongodb_cache', 0, True ],
            [ 'mongodb_complex_integers_key_hashed', 1, False ],
            [ 'mongodb_complex_integers_key_cache', 1, False ],
            [ 'mongodb_complex_mixed_key_hashed', 2, False ],
            [ 'mongodb_complex_mixed_key_cache', 2, False ],
        ])

    if args.use_mongo_user:
        dictionaries.extend([
            [ 'mongodb_user_flat', 0, True ],
        ])

    if args.use_lib:
        dictionaries.extend([
            # [ 'library_flat', 0, True ],
            # [ 'library_hashed', 0, True ],
            # [ 'library_cache', 0, True ],
            # [ 'library_complex_integers_key_hashed', 1, False ],
            # [ 'library_complex_integers_key_cache', 1, False ],
            # [ 'library_complex_mixed_key_hashed', 2, False ],
            # [ 'library_complex_mixed_key_cache', 2, False ],
            # [ 'library_c_flat', 0, True ],
            # [ 'library_c_hashed', 0, True ],
            # [ 'library_c_cache', 0, True ],
            # [ 'library_c_complex_integers_key_hashed', 1, False ],
            # [ 'library_c_complex_integers_key_cache', 1, False ],
            # [ 'library_c_complex_mixed_key_hashed', 2, False ],
            # [ 'library_c_complex_mixed_key_cache', 2, False ],
        ])


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


def dump_report(destination, suite, test_case, report):
    if destination is not None:
        destination_file = os.path.join(destination, suite, test_case + ".xml")
        destination_dir = os.path.dirname(destination_file)
        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)
        with open(destination_file, 'w') as report_file:
            report_root = et.Element("testsuites", attrib = {'name': 'ClickHouse External Dictionaries Tests'})
            report_suite = et.Element("testsuite", attrib = {"name": suite})
            report_suite.append(report)
            report_root.append(report_suite)
            report_file.write(et.tostring(report_root, encoding = "UTF-8", xml_declaration=True, pretty_print=True))


def call(args, out_filename):
    with open(out_filename, 'w') as file:
        subprocess.check_call(args, stdout=file)


def generate_data(args):
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
    system('cat {source} | {ch} --port={port} -m -n --query "'
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
              '"'.format(source = args.source, ch = args.client, port = args.port))

    # generate 3 files with different key types
    print 'Creating .tsv files'
    file_source_query = 'select %s from test.dictionary_source format TabSeparated;'
    for file, keys in zip(files, key_columns):
        query = file_source_query % comma_separated(chain(keys, columns(), [ 'Parent' ] if 1 == len(keys) else []))
        call([ args.client, '--port', args.port, '--query', query ], 'generated/' + file)

    # create MySQL table from complete_query
    if not args.no_mysql:
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
                  'load data local infile \'{0}/source.tsv\' into table test.dictionary_source;" | mysql $MYSQL_OPTIONS --local-infile=1'
                  .format(prefix), shell=True)

    # create MongoDB collection from complete_query via JSON file
    if not args.no_mongo:
        print 'Creating MongoDB test_user'
        subprocess.call([ 'mongo', '--eval', 'db.createUser({ user: "test_user", pwd: "test_pass", roles: [ { role: "readWrite", db: "test" } ] })' ])

        print 'Creating MongoDB collection'
        table_rows = json.loads(subprocess.check_output([
            args.client,
            '--port',
            args.port,
            '--output_format_json_quote_64bit_integers',
            '0',
            '--query',
            "select * from test.dictionary_source where not ignore(" \
                "concat('new Date(\\'', toString(Date_), '\\')') as Date_, " \
                "concat('new ISODate(\\'', replaceOne(toString(DateTime_, 'UTC'), ' ', 'T'), 'Z\\')') as DateTime_" \
            ") format JSON"
        ]))['data']

        source_for_mongo = json.dumps(table_rows).replace(')"', ')').replace('"new', 'new')
        open('generated/full.json', 'w').write('db.dictionary_source.drop(); db.dictionary_source.insert(%s);' % source_for_mongo)
        result = system('cat {0}/full.json | mongo --quiet > /dev/null'.format(args.generated))
        if result != 0:
            print 'Could not create MongoDB collection'
            exit(-1)


def generate_dictionaries(args):
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
        <host>localhost</host>
        <port>%s</port>
        <user>default</user>
        <password></password>
        <db>test</db>
        <table>dictionary_source</table>
    </clickhouse>
    ''' % args.port

    source_mysql = '''
    <mysql>
        <replica>
            <priority>1</priority>
            <host>127.0.0.1</host>
            <port>3333</port>   <!-- Wrong port, for testing basic failover to work. -->
        </replica>
        <replica>
            <priority>2</priority>
            <host>localhost</host>
            <port>3306</port>
        </replica>
        <user>root</user>
        <password></password>
        <db>test</db>
        <table>dictionary_source</table>
    </mysql>
    '''

    source_mongodb = '''
    <mongodb>
        <host>{mongo_host}</host>
        <port>27017</port>
        <user></user>
        <password></password>
        <db>test</db>
        <collection>dictionary_source</collection>
    </mongodb>
    '''.format(mongo_host=args.mongo_host)

    source_mongodb_user = '''
    <mongodb>
        <host>{mongo_host}</host>
        <port>27017</port>
        <user>test_user</user>
        <password>test_pass</password>
        <db>test</db>
        <collection>dictionary_source</collection>
    </mongodb>
    '''.format(mongo_host=args.mongo_host)

    source_executable = '''
    <executable>
        <command>cat %s</command>
        <format>TabSeparated</format>
    </executable>
    '''

    # ignore stdin, then print file
    source_executable_cache = '''
    <executable>
        <command>cat ->/dev/null; cat %s</command>
        <format>TabSeparated</format>
    </executable>
    '''

    source_http = '''
    <http>
        <url>http://{http_host}:{http_port}{http_path}%s</url>
        <format>TabSeparated</format>
    </http>
    '''.format(http_host=args.http_host, http_port=args.http_port, http_path=args.http_path)

    source_https = '''
    <http>
        <url>https://{https_host}:{https_port}{https_path}%s</url>
        <format>TabSeparated</format>
    </http>
    '''.format(https_host=args.https_host, https_port=args.https_port, https_path=args.https_path)

    source_library = '''
    <library>
        <path>{filename}</path>
    </library>
    '''.format(filename=os.path.abspath('../../../build/dbms/tests/external_dictionaries/dictionary_library/libdictionary_library.so'))

    # Todo?
    #source_library_c = '''
    #<library>
    #    <path>{filename}</path>
    #</library>
    #'''.format(filename=os.path.abspath('../../../build/dbms/tests/external_dictionaries/dict_lib/libdict_library_c.so'))


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
        [ source_executable % (generated_prefix + files[0]), layout_flat ],

        [ source_file % (generated_prefix + files[0]), layout_hashed],
        [ source_clickhouse, layout_hashed ],
        [ source_executable % (generated_prefix + files[0]), layout_hashed ],

        [ source_clickhouse, layout_cache ],
        [ source_executable_cache % (generated_prefix + files[0]), layout_cache ],

        # Complex key dictionaries with (UInt8, UInt8) key
        [ source_file % (generated_prefix + files[1]), layout_complex_key_hashed],
        [ source_clickhouse, layout_complex_key_hashed ],
        [ source_executable % (generated_prefix + files[1]), layout_complex_key_hashed ],

        [ source_clickhouse, layout_complex_key_cache ],
        [ source_executable_cache % (generated_prefix + files[1]), layout_complex_key_cache ],

        # Complex key dictionaries with (String, UInt8) key
        [ source_file % (generated_prefix + files[2]), layout_complex_key_hashed],
        [ source_clickhouse, layout_complex_key_hashed ],
        [ source_executable % (generated_prefix + files[2]), layout_complex_key_hashed ],

        [ source_clickhouse, layout_complex_key_cache ],
        [ source_executable_cache % (generated_prefix + files[2]), layout_complex_key_cache ],
    ]


    if not args.no_http:
        sources_and_layouts.extend([
        [ source_http % (files[0]), layout_flat ],
        [ source_http % (files[0]), layout_hashed ],
        [ source_http % (files[0]), layout_cache ],
        [ source_http % (files[1]), layout_complex_key_hashed ],
        [ source_http % (files[1]), layout_complex_key_cache ],
        [ source_http % (files[2]), layout_complex_key_hashed ],
        [ source_http % (files[2]), layout_complex_key_cache ],
    ])

    if not args.no_https:
        sources_and_layouts.extend([
        [ source_https % (files[0]), layout_flat ],
        [ source_https % (files[0]), layout_hashed ],
        [ source_https % (files[0]), layout_cache ],
    ])

    if not args.no_mysql:
        sources_and_layouts.extend([
        [ source_mysql, layout_flat ],
        [ source_mysql, layout_hashed ],
        [ source_mysql, layout_cache ],
        [ source_mysql, layout_complex_key_hashed ],
        [ source_mysql, layout_complex_key_cache ],
        [ source_mysql, layout_complex_key_hashed ],
        [ source_mysql, layout_complex_key_cache ],
    ])

    if not args.no_mongo:
        sources_and_layouts.extend([
        [ source_mongodb, layout_flat ],
        [ source_mongodb, layout_hashed ],
        [ source_mongodb, layout_cache ],
        [ source_mongodb, layout_complex_key_cache ],
        [ source_mongodb, layout_complex_key_hashed ],
        [ source_mongodb, layout_complex_key_hashed ],
        [ source_mongodb, layout_complex_key_cache ],
    ])

    if args.use_mongo_user:
        sources_and_layouts.extend( [
        [ source_mongodb_user, layout_flat ],
    ])

    if args.use_lib:
        sources_and_layouts.extend([
        #[ source_library, layout_flat ],
        #[ source_library, layout_hashed ],
        #[ source_library, layout_cache ],
        #[ source_library, layout_complex_key_cache ],
        #[ source_library, layout_complex_key_hashed ],
        #[ source_library, layout_complex_key_hashed ],
        #[ source_library, layout_complex_key_cache ],
        #[ source_library_c, layout_flat ],
        #[ source_library_c, layout_hashed ],
        #[ source_library_c, layout_cache ],
        #[ source_library_c, layout_complex_key_cache ],
        #[ source_library_c, layout_complex_key_hashed ],
        #[ source_library_c, layout_complex_key_hashed ],
        #[ source_library_c, layout_complex_key_cache ],
    ])

    for (name, key_idx, has_parent), (source, layout) in zip(dictionaries, sources_and_layouts):
        filename = os.path.join(args.generated, 'dictionary_%s.xml' % name)
        with open(filename, 'w') as file:
            dictionary_xml = dictionary_skeleton.format(
                key = keys[key_idx], parent = parent_attribute if has_parent else '', **locals())
            file.write(dictionary_xml)


def run_tests(args):
    if not args.no_http:
        http_server = subprocess.Popen(["python", "http_server.py", "--port", str(args.http_port), "--host", args.http_host]);
        @atexit.register
        def http_killer():
           http_server.kill()

    if not args.no_https:
        https_server = subprocess.Popen(["python", "http_server.py", "--port", str(args.https_port), "--host", args.https_host, '--https']);
        @atexit.register
        def https_killer():
           https_server.kill()

    keys = [ 'toUInt64(n)', '(n, n)', '(toString(n), n)' ]
    dict_get_query_skeleton = "select dictGet{type}('{name}', '{type}_', {key}) from system.one array join range(8) as n;"
    dict_has_query_skeleton = "select dictHas('{name}', {key}) from system.one array join range(8) as n;"
    dict_get_or_default_query_skeleton = "select dictGet{type}OrDefault('{name}', '{type}_', {key}, to{type}({default})) from system.one array join range(8) as n;"
    dict_hierarchy_query_skeleton = "select dictGetHierarchy('{name}' as d, key), dictIsIn(d, key, toUInt64(1)), dictIsIn(d, key, key) from system.one array join range(toUInt64(8)) as key;"

    def test_query(dict, query, reference, name):
        global failures
        global SERVER_DIED

        print "{0:100}".format('Dictionary: ' + dict + ' Name: ' + name + ": "),
        sys.stdout.flush()
        report_testcase = et.Element("testcase", attrib = {"name": name})

        reference_file = os.path.join(args.reference, reference) + '.reference'
        stdout_file = os.path.join(args.reference, reference) + '.stdout'
        stderr_file = os.path.join(args.reference, reference) + '.stderr'

        command = '{ch} --port {port} --query "{query}" > {stdout_file}  2> {stderr_file}'.format(ch = args.client, port = args.port, query = query, stdout_file = stdout_file, stderr_file = stderr_file)
        proc = Popen(command, shell = True)
        start_time = datetime.now()
        while (datetime.now() - start_time).total_seconds() < args.timeout and proc.poll() is None:
            sleep(0.01)

        if proc.returncode is None:
            try:
                proc.kill()
            except OSError as e:
                if e.errno != ESRCH:
                    raise

            failure = et.Element("failure", attrib = {"message": "Timeout"})
            report_testcase.append(failure)
            failures = failures + 1
            print("{0} - Timeout!".format(MSG_FAIL))
        else:
            stdout = open(stdout_file, 'r').read() if os.path.exists(stdout_file) else ''
            stdout = unicode(stdout, errors='replace', encoding='utf-8')
            stderr = open(stderr_file, 'r').read() if os.path.exists(stderr_file) else ''
            stderr = unicode(stderr, errors='replace', encoding='utf-8')

            if proc.returncode != 0:
                failure = et.Element("failure", attrib = {"message": "return code {}".format(proc.returncode)})
                report_testcase.append(failure)

                stdout_element = et.Element("system-out")
                stdout_element.text = et.CDATA(stdout)
                report_testcase.append(stdout_element)

                failures = failures + 1
                print("{0} - return code {1}".format(MSG_FAIL, proc.returncode))

                if stderr:
                    stderr_element = et.Element("system-err")
                    stderr_element.text = et.CDATA(stderr)
                    report_testcase.append(stderr_element)
                    print(stderr)

                if 'Connection refused' in stderr or 'Attempt to read after eof' in stderr:
                    SERVER_DIED = True

            elif stderr:
                failure = et.Element("failure", attrib = {"message": "having stderror"})
                report_testcase.append(failure)

                stderr_element = et.Element("system-err")
                stderr_element.text = et.CDATA(stderr)
                report_testcase.append(stderr_element)

                failures = failures + 1
                print("{0} - having stderror:\n{1}".format(MSG_FAIL, stderr.encode('utf-8')))
            elif 'Exception' in stdout:
                failure = et.Element("error", attrib = {"message": "having exception"})
                report_testcase.append(failure)

                stdout_element = et.Element("system-out")
                stdout_element.text = et.CDATA(stdout)
                report_testcase.append(stdout_element)

                failures = failures + 1
                print("{0} - having exception:\n{1}".format(MSG_FAIL, stdout.encode('utf-8')))
            elif not os.path.isfile(reference_file):
                skipped = et.Element("skipped", attrib = {"message": "no reference file"})
                report_testcase.append(skipped)
                print("{0} - no reference file".format(MSG_UNKNOWN))
            else:
                (diff, _) = Popen(['diff', reference_file, stdout_file], stdout = PIPE).communicate()

                if diff:
                    failure = et.Element("failure", attrib = {"message": "result differs with reference"})
                    report_testcase.append(failure)

                    stdout_element = et.Element("system-out")
                    stdout_element.text = et.CDATA(diff)
                    report_testcase.append(stdout_element)

                    failures = failures + 1
                    print("{0} - result differs with reference:\n{1}".format(MSG_FAIL, diff))
                else:
                    print(MSG_OK)
                    if os.path.exists(stdout_file):
                        os.remove(stdout_file)
                    if os.path.exists(stderr_file):
                        os.remove(stderr_file)

        dump_report(args.output, dict, name, report_testcase)


    print 'Waiting for dictionaries to load...'
    time.sleep(wait_for_loading_sleep_time_sec)

    # the actual tests
    for (name, key_idx, has_parent) in dictionaries:
        if SERVER_DIED and not args.no_break:
            break
        key = keys[key_idx]
        print 'Testing dictionary', name

        # query dictHas
        test_query(name, dict_has_query_skeleton.format(**locals()), 'has', 'dictHas')

        # query dictGet*
        for type, default in zip(types, explicit_defaults):
            if SERVER_DIED and not args.no_break:
                break
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

    if failures > 0:
        print(colored("\nHaving {0} errors!".format(failures), "red", attrs=["bold"]))
        sys.exit(1)
    else:
        print(colored("\nAll tests passed.", "green", attrs=["bold"]))
        sys.exit(0)


def main(args):
    generate_structure(args)
    generate_dictionaries(args)
    generate_data(args)
    run_tests(args)


if __name__ == '__main__':
    parser = ArgumentParser(description = 'ClickHouse external dictionaries tests')
    parser.add_argument('-s', '--source', default = 'source.tsv', help = 'Path to source data')
    parser.add_argument('-g', '--generated', default = 'generated', help = 'Path to directory with generated data')
    parser.add_argument('-r', '--reference', default = 'reference', help = 'Path to directory with reference data')
    parser.add_argument('-c', '--client', default = 'clickhouse-client', help = 'Client program')
    parser.add_argument('-p', '--port', default = '9001', help = 'ClickHouse port')
    parser.add_argument('-o', '--output', default = 'output', help = 'Output xUnit compliant test report directory')
    parser.add_argument('-t', '--timeout', type = int, default = 10, help = 'Timeout for each test case in seconds')

    # Not complete disable. Now only skip data prepare. Todo: skip requests too. Now can be used with --no_break
    parser.add_argument('--use_lib',   action='store_true', help = 'Use lib dictionaries')
    parser.add_argument('--no_mysql', action='store_true', help = 'Dont use mysql dictionaries')
    parser.add_argument('--no_mongo', action='store_true', help = 'Dont use mongodb dictionaries')
    parser.add_argument('--mongo_host', default = 'localhost', help = 'mongo server host')
    parser.add_argument('--use_mongo_user', action='store_true', help = 'Test mongodb with user-pass')

    parser.add_argument('--no_http', action='store_true', help = 'Dont use http dictionaries')
    parser.add_argument('--http_port', default = 58000, help = 'http server port')
    parser.add_argument('--http_host', default = 'localhost', help = 'http server host')
    parser.add_argument('--http_path', default = '/generated/', help = 'http server path')
    parser.add_argument('--no_https', action='store_true', help = 'Dont use https dictionaries')
    parser.add_argument('--https_port', default = 58443, help = 'https server port')
    parser.add_argument('--https_host', default = 'localhost', help = 'https server host')
    parser.add_argument('--https_path', default = '/generated/', help = 'https server path')
    parser.add_argument('--no_break', action='store_true', help = 'Dont stop on errors')

    args = parser.parse_args()

    main(args)

