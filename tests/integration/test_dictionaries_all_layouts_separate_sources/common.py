import os
import shutil

from helpers.dictionary import Field, Row, Dictionary, DictionaryStructure, Layout

KEY_FIELDS = {
    "simple": [Field("KeyField", "UInt64", is_key=True, default_value_for_get=9999999)],
    "complex": [
        Field("KeyField1", "UInt64", is_key=True, default_value_for_get=9999999),
        Field("KeyField2", "String", is_key=True, default_value_for_get="xxxxxxxxx"),
    ],
    "ranged": [
        Field("KeyField1", "UInt64", is_key=True),
        Field("KeyField2", "Date", is_range_key=True),
    ],
}

START_FIELDS = {
    "simple": [],
    "complex": [],
    "ranged": [
        Field("StartDate", "Date", range_hash_type="min"),
        Field("EndDate", "Date", range_hash_type="max"),
    ],
}

MIDDLE_FIELDS = [
    Field("UInt8_", "UInt8", default_value_for_get=55),
    Field("UInt16_", "UInt16", default_value_for_get=66),
    Field("UInt32_", "UInt32", default_value_for_get=77),
    Field("UInt64_", "UInt64", default_value_for_get=88),
    Field("Int8_", "Int8", default_value_for_get=-55),
    Field("Int16_", "Int16", default_value_for_get=-66),
    Field("Int32_", "Int32", default_value_for_get=-77),
    Field("Int64_", "Int64", default_value_for_get=-88),
    Field(
        "UUID_", "UUID", default_value_for_get="550e8400-0000-0000-0000-000000000000"
    ),
    Field("Date_", "Date", default_value_for_get="2018-12-30"),
    Field("DateTime_", "DateTime", default_value_for_get="2018-12-30 00:00:00"),
    Field("String_", "String", default_value_for_get="hi"),
    Field("Float32_", "Float32", default_value_for_get=555.11),
    Field("Float64_", "Float64", default_value_for_get=777.11),
]

END_FIELDS = {
    "simple": [
        Field("ParentKeyField", "UInt64", default_value_for_get=444, hierarchical=True)
    ],
    "complex": [],
    "ranged": [],
}

LAYOUTS_SIMPLE = ["flat", "hashed", "cache", "direct"]
LAYOUTS_COMPLEX = ["complex_key_hashed", "complex_key_cache", "complex_key_direct"]
LAYOUTS_RANGED = ["range_hashed"]

VALUES = {
    "simple": [
        [
            1,
            22,
            333,
            4444,
            55555,
            -6,
            -77,
            -888,
            -999,
            "550e8400-e29b-41d4-a716-446655440003",
            "1973-06-28",
            "1985-02-28 23:43:25",
            "hello",
            22.543,
            3332154213.4,
            0,
        ],
        [
            2,
            3,
            4,
            5,
            6,
            -7,
            -8,
            -9,
            -10,
            "550e8400-e29b-41d4-a716-446655440002",
            "1978-06-28",
            "1986-02-28 23:42:25",
            "hello",
            21.543,
            3222154213.4,
            1,
        ],
    ],
    "complex": [
        [
            1,
            "world",
            22,
            333,
            4444,
            55555,
            -6,
            -77,
            -888,
            -999,
            "550e8400-e29b-41d4-a716-446655440003",
            "1973-06-28",
            "1985-02-28 23:43:25",
            "hello",
            22.543,
            3332154213.4,
        ],
        [
            2,
            "qwerty2",
            52,
            2345,
            6544,
            9191991,
            -2,
            -717,
            -81818,
            -92929,
            "550e8400-e29b-41d4-a716-446655440007",
            "1975-09-28",
            "2000-02-28 23:33:24",
            "my",
            255.543,
            3332221.44,
        ],
    ],
    "ranged": [
        [
            1,
            "2019-02-10",
            "2019-02-01",
            "2019-02-28",
            22,
            333,
            4444,
            55555,
            -6,
            -77,
            -888,
            -999,
            "550e8400-e29b-41d4-a716-446655440003",
            "1973-06-28",
            "1985-02-28 23:43:25",
            "hello",
            22.543,
            3332154213.4,
        ],
        [
            2,
            "2019-04-10",
            "2019-04-01",
            "2019-04-28",
            11,
            3223,
            41444,
            52515,
            -65,
            -747,
            -8388,
            -9099,
            "550e8400-e29b-41d4-a716-446655440004",
            "1973-06-29",
            "2002-02-28 23:23:25",
            "!!!!",
            32.543,
            3332543.4,
        ],
    ],
}


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DICT_CONFIG_PATH = os.path.join(SCRIPT_DIR, "configs", "dictionaries")


class BaseLayoutTester:
    def __init__(self, test_name):
        self.test_name = test_name
        self.layouts = []

    def get_dict_directory(self):
        return os.path.join(DICT_CONFIG_PATH, self.test_name)

    def cleanup(self):
        shutil.rmtree(self.get_dict_directory(), ignore_errors=True)
        os.makedirs(self.get_dict_directory())

    def list_dictionaries(self):
        dictionaries = []
        directory = self.get_dict_directory()
        for fname in os.listdir(directory):
            dictionaries.append(os.path.join(directory, fname))
        return dictionaries

    def create_dictionaries(self, source_):
        for layout in self.layouts:
            if source_.compatible_with_layout(Layout(layout)):
                self.layout_to_dictionary[layout] = self.get_dict(
                    source_, Layout(layout), self.fields
                )

    def prepare(self, cluster_):
        for _, dictionary in list(self.layout_to_dictionary.items()):
            dictionary.prepare_source(cluster_)
            dictionary.load_data(self.data)

    def get_dict(self, source, layout, fields, suffix_name=""):
        structure = DictionaryStructure(layout, fields)
        dict_name = source.name + "_" + layout.name + "_" + suffix_name
        dict_path = os.path.join(self.get_dict_directory(), dict_name + ".xml")
        dictionary = Dictionary(
            dict_name, structure, source, dict_path, "table_" + dict_name, fields
        )
        dictionary.generate_config()
        return dictionary


class SimpleLayoutTester(BaseLayoutTester):
    def __init__(self, test_name):
        self.fields = (
            KEY_FIELDS["simple"]
            + START_FIELDS["simple"]
            + MIDDLE_FIELDS
            + END_FIELDS["simple"]
        )
        self.values = VALUES["simple"]
        self.data = [Row(self.fields, vals) for vals in self.values]
        self.layout_to_dictionary = dict()
        self.test_name = test_name
        self.layouts = LAYOUTS_SIMPLE

    def execute(self, layout_name, node):
        if layout_name not in self.layout_to_dictionary:
            raise RuntimeError("Source doesn't support layout: {}".format(layout_name))

        dct = self.layout_to_dictionary[layout_name]

        node.query("system reload dictionaries")
        queries_with_answers = []

        for row in self.data:
            for field in self.fields:
                if not field.is_key:
                    for query in dct.get_select_get_queries(field, row):
                        queries_with_answers.append(
                            (query, row.get_value_by_name(field.name))
                        )

                    for query in dct.get_select_has_queries(field, row):
                        queries_with_answers.append((query, 1))

                    for query in dct.get_select_get_or_default_queries(field, row):
                        queries_with_answers.append(
                            (query, field.default_value_for_get)
                        )

        for query in dct.get_hierarchical_queries(self.data[0]):
            queries_with_answers.append((query, [1]))

        for query in dct.get_hierarchical_queries(self.data[1]):
            queries_with_answers.append((query, [2, 1]))

        for query in dct.get_is_in_queries(self.data[0], self.data[1]):
            queries_with_answers.append((query, 0))

        for query in dct.get_is_in_queries(self.data[1], self.data[0]):
            queries_with_answers.append((query, 1))

        for query, answer in queries_with_answers:
            # print query
            if isinstance(answer, list):
                answer = str(answer).replace(" ", "")
            answer = str(answer) + "\n"
            node_answer = node.query(query)
            assert (
                str(node_answer).strip() == answer.strip()
            ), f"Expected '{answer.strip()}', got '{node_answer.strip()}' in query '{query}'"


class ComplexLayoutTester(BaseLayoutTester):
    def __init__(self, test_name):
        self.fields = (
            KEY_FIELDS["complex"]
            + START_FIELDS["complex"]
            + MIDDLE_FIELDS
            + END_FIELDS["complex"]
        )
        self.values = VALUES["complex"]
        self.data = [Row(self.fields, vals) for vals in self.values]
        self.layout_to_dictionary = dict()
        self.test_name = test_name
        self.layouts = LAYOUTS_COMPLEX

    def execute(self, layout_name, node):
        if layout_name not in self.layout_to_dictionary:
            raise RuntimeError("Source doesn't support layout: {}".format(layout_name))

        dct = self.layout_to_dictionary[layout_name]

        node.query("system reload dictionaries")
        queries_with_answers = []

        for row in self.data:
            for field in self.fields:
                if not field.is_key:
                    for query in dct.get_select_get_queries(field, row):
                        queries_with_answers.append(
                            (query, row.get_value_by_name(field.name))
                        )

                    for query in dct.get_select_has_queries(field, row):
                        queries_with_answers.append((query, 1))

                    for query in dct.get_select_get_or_default_queries(field, row):
                        queries_with_answers.append(
                            (query, field.default_value_for_get)
                        )

        for query, answer in queries_with_answers:
            # print query
            node_answer = node.query(query)
            answer = str(answer) + "\n"
            assert (
                node_answer == answer
            ), f"Expected '{answer.strip()}', got '{node_answer.strip()}' in query '{query}'"


class RangedLayoutTester(BaseLayoutTester):
    def __init__(self, test_name):
        self.fields = (
            KEY_FIELDS["ranged"]
            + START_FIELDS["ranged"]
            + MIDDLE_FIELDS
            + END_FIELDS["ranged"]
        )
        self.values = VALUES["ranged"]
        self.data = [Row(self.fields, vals) for vals in self.values]
        self.layout_to_dictionary = dict()
        self.test_name = test_name
        self.layouts = LAYOUTS_RANGED

    def execute(self, layout_name, node):

        if layout_name not in self.layout_to_dictionary:
            raise RuntimeError("Source doesn't support layout: {}".format(layout_name))

        dct = self.layout_to_dictionary[layout_name]

        node.query("system reload dictionaries")

        queries_with_answers = []
        for row in self.data:
            for field in self.fields:
                if not field.is_key and not field.is_range:
                    for query in dct.get_select_get_queries(field, row):
                        queries_with_answers.append(
                            (query, row.get_value_by_name(field.name))
                        )

        for query, answer in queries_with_answers:
            # print query
            node_answer = node.query(query)
            answer = str(answer) + "\n"
            assert (
                node_answer == answer
            ), f"Expected '{answer.strip()}', got '{node_answer.strip()}' in query '{query}'"
