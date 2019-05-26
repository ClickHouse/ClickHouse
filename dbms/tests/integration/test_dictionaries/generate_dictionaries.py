import os
import glob
import difflib

files = ['key_simple.tsv', 'key_complex_integers.tsv', 'key_complex_mixed.tsv']

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
    '1', '1', '1', '',
    '-1', '-1', '-1', '-1',
    '2.71828', '2.71828',
    'implicit-default',
    '2015-11-25', ''
]


def generate_structure():
    # [ name, key_type, has_parent ]
    return [
        # Simple key dictionaries
        ['clickhouse_flat', 0, True],
        ['clickhouse_hashed', 0, True],
        ['clickhouse_cache', 0, True],

        # Complex key dictionaries with (UInt8, UInt8) key
        ['clickhouse_complex_integers_key_hashed', 1, False],
        ['clickhouse_complex_integers_key_cache', 1, False],

        # Complex key dictionaries with (String, UInt8) key
        ['clickhouse_complex_mixed_key_hashed', 2, False],
        ['clickhouse_complex_mixed_key_cache', 2, False],

        # Range hashed dictionary
        ['clickhouse_range_hashed', 3, False],
    ]


def generate_dictionaries(path, structure):
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

    dictionary_skeleton = \
        dictionary_skeleton % reduce(lambda xml, (type, default): xml + attribute_skeleton % (type, type, default),
                                     zip(types, implicit_defaults), '')

    source_clickhouse = '''
    <clickhouse>
        <host>localhost</host>
        <port>9000</port>
        <user>default</user>
        <password></password>
        <db>test</db>
        <table>dictionary_source</table>
    </clickhouse>
    '''

    layout_flat = '<flat />'
    layout_hashed = '<hashed />'
    layout_cache = '<cache><size_in_cells>128</size_in_cells></cache>'
    layout_complex_key_hashed = '<complex_key_hashed />'
    layout_complex_key_cache = '<complex_key_cache><size_in_cells>128</size_in_cells></complex_key_cache>'
    layout_range_hashed = '<range_hashed />'

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

    key_range_hashed = '''
    <id>
        <name>id</name>
    </id>
    <range_min>
        <name>StartDate</name>
    </range_min>
    <range_max>
        <name>EndDate</name>
    </range_max>
    '''

    keys = [key_simple, key_complex_integers, key_complex_mixed, key_range_hashed]

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
        [source_clickhouse, layout_flat],
        [source_clickhouse, layout_hashed],
        [source_clickhouse, layout_cache],

        # Complex key dictionaries with (UInt8, UInt8) key
        [source_clickhouse, layout_complex_key_hashed],
        [source_clickhouse, layout_complex_key_cache],

        # Complex key dictionaries with (String, UInt8) key
        [source_clickhouse, layout_complex_key_hashed],
        [source_clickhouse, layout_complex_key_cache],

        # Range hashed dictionary
        [source_clickhouse, layout_range_hashed],
    ]

    file_names = []

    # Add ready dictionaries.
    file_names.extend(glob.glob(os.path.join(path, "*dictionary_preset*.xml")))

    # Generate dictionaries.
    for (name, key_idx, has_parent), (source, layout) in zip(structure, sources_and_layouts):
        filename = os.path.join(path, 'dictionary_%s.xml' % name)
        file_names.append(filename)
        with open(filename, 'w') as file:
            dictionary_xml = dictionary_skeleton.format(
                key=keys[key_idx], parent=parent_attribute if has_parent else '', **locals())
            file.write(dictionary_xml)

    return file_names


class DictionaryTestTable:
    def __init__(self, source_file_name):
        self.structure = '''id UInt64, key0 UInt8, key0_str String, key1 UInt8,
                    StartDate Date, EndDate Date,
                    UInt8_ UInt8, UInt16_ UInt16, UInt32_ UInt32, UInt64_ UInt64,
                    Int8_ Int8, Int16_ Int16, Int32_ Int32, Int64_ Int64,
                    Float32_ Float32, Float64_ Float64,
                    String_ String,
                    Date_ Date, DateTime_ DateTime, Parent UInt64'''

        self.names_and_types = map(str.split, self.structure.split(','))
        self.keys_names_and_types = self.names_and_types[:6]
        self.values_names_and_types = self.names_and_types[6:]
        self.source_file_name = source_file_name
        self.rows = None

    def create_clickhouse_source(self, instance):
        query = '''
        create database if not exists test;
        drop table if exists test.dictionary_source;
        create table test.dictionary_source (%s) engine=Log; insert into test.dictionary_source values %s ;
        '''

        types = tuple(pair[1] for pair in self.names_and_types)

        with open(self.source_file_name) as source_file:
            lines = source_file.read().split('\n')
        lines = tuple(filter(len, lines))

        self.rows = []

        def wrap_value(pair):
            value, type = pair
            return "'" + value + "'" if type in ('String', 'Date', 'DateTime') else value

        def make_tuple(line):
            row = tuple(line.split('\t'))
            self.rows.append(row)
            return '(' + ','.join(map(wrap_value, zip(row, types))) + ')'

        values = ','.join(map(make_tuple, lines))
        print query % (self.structure, values)
        instance.query(query % (self.structure, values))

    def get_structure_for_keys(self, keys, enable_parent=True):
        structure = ','.join(name + ' ' + type for name, type in self.keys_names_and_types if name in keys)
        return structure + ', ' + ','.join(name + ' ' + type for name, type in self.values_names_and_types
                                           if enable_parent or name != 'Parent')

    def _build_line_from_row(self, row, names):
        return '\t'.join((value for value, (name, type) in zip(row, self.names_and_types) if name in set(names)))

    def compare_rows_by_keys(self, keys, values, lines, add_not_found_rows=True):
        rows = [line.rstrip('\n').split('\t') for line in lines]
        diff = []
        matched = []
        lines_map = {self._build_line_from_row(row, keys): self._build_line_from_row(row, values) for row in self.rows}
        for row in rows:
            key = '\t'.join(row[:len(keys)])
            value = '\t'.join(row[len(keys):])
            if key in lines_map.keys():
                pattern_value = lines_map[key]
                del lines_map[key]
                if not value == pattern_value:
                    diff.append((key + '\t' + value, key + '\t' + pattern_value))
                else:
                    matched.append((key + '\t' + value, key + '\t' + pattern_value))
            else:
                diff.append((key + '\t' + value, ''))

        if add_not_found_rows:
            for key, value in lines_map.items():
                diff.append(('', key + '\t' + value))

        if not diff:
            return None

        diff += matched
        left_lines = tuple(pair[0] for pair in diff)
        right_lines = tuple(pair[1] for pair in diff)
        return left_lines, right_lines

    def compare_by_keys(self, keys, lines, with_parent_column=True, add_not_found_rows=True):
        values = [name for name, type in self.values_names_and_types if with_parent_column or name != 'Parent']
        return self.compare_rows_by_keys(keys, values, lines, add_not_found_rows)

    def process_diff(self, diff):
        if not diff:
            return ''
        left_lines, right_lines = diff
        args = {'fromfile': 'received', 'tofile': 'expected', 'lineterm': ''}
        return '\n'.join(tuple(difflib.context_diff(left_lines, right_lines, **args))[:])
