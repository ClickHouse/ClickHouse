# -*- coding: utf-8 -*-
import copy


class Layout(object):
    LAYOUTS_STR_DICT = {
        "flat": "<flat/>",
        "hashed": "<hashed/>",
        "cache": "<cache><size_in_cells>128</size_in_cells></cache>",
        "ssd_cache": "<ssd_cache><path>/etc/clickhouse-server/dictionaries/all</path></ssd_cache>",
        "complex_key_hashed": "<complex_key_hashed/>",
        "complex_key_hashed_one_key": "<complex_key_hashed/>",
        "complex_key_hashed_two_keys": "<complex_key_hashed/>",
        "complex_key_cache": "<complex_key_cache><size_in_cells>128</size_in_cells></complex_key_cache>",
        "complex_key_ssd_cache": "<complex_key_ssd_cache><path>/etc/clickhouse-server/dictionaries/all</path></complex_key_ssd_cache>",
        "range_hashed": "<range_hashed/>",
        "direct": "<direct/>",
        "complex_key_direct": "<complex_key_direct/>",
    }

    def __init__(self, name):
        self.name = name
        self.is_complex = False
        self.is_simple = False
        self.is_ranged = False
        if self.name.startswith("complex"):
            self.layout_type = "complex"
            self.is_complex = True
        elif name.startswith("range"):
            self.layout_type = "ranged"
            self.is_ranged = True
        else:
            self.layout_type = "simple"
            self.is_simple = True

    def get_str(self):
        return self.LAYOUTS_STR_DICT[self.name]

    def get_key_block_name(self):
        if self.is_complex:
            return "key"
        return "id"


class Row(object):
    def __init__(self, fields, values):
        self.data = {}
        for field, value in zip(fields, values):
            self.data[field.name] = value

    def has_field(self, name):
        return name in self.data

    def get_value_by_name(self, name):
        return self.data[name]

    def set_value(self, name, value):
        self.data[name] = value


class Field(object):
    def __init__(
        self,
        name,
        field_type,
        is_key=False,
        is_range_key=False,
        default=None,
        hierarchical=False,
        range_hash_type=None,
        default_value_for_get=None,
    ):
        self.name = name
        self.field_type = field_type
        self.is_key = is_key
        self.default = default
        self.hierarchical = hierarchical
        self.range_hash_type = range_hash_type
        self.is_range = self.range_hash_type is not None
        self.is_range_key = is_range_key
        self.default_value_for_get = default_value_for_get

    def get_attribute_str(self):
        return """
            <attribute>
              <name>{name}</name>
              <type>{field_type}</type>
              <null_value>{default}</null_value>
              <hierarchical>{hierarchical}</hierarchical>
            </attribute>""".format(
            name=self.name,
            field_type=self.field_type,
            default=self.default if self.default else "",
            hierarchical="true" if self.hierarchical else "false",
        )

    def get_simple_index_str(self):
        return "<name>{name}</name>".format(name=self.name)

    def get_range_hash_str(self):
        if not self.range_hash_type:
            raise Exception("Field {} is not range hashed".format(self.name))
        return """
            <range_{type}>
                <name>{name}</name>
            </range_{type}>
        """.format(
            type=self.range_hash_type, name=self.name
        )


class DictionaryStructure(object):
    def __init__(self, layout, fields):
        self.layout = layout
        self.keys = []
        self.range_key = None
        self.ordinary_fields = []
        self.range_fields = []
        self.has_hierarchy = False

        for field in fields:
            if field.is_key:
                self.keys.append(field)
            elif field.is_range:
                self.range_fields.append(field)
            else:
                self.ordinary_fields.append(field)

            if field.hierarchical:
                self.has_hierarchy = True

            if field.is_range_key:
                if self.range_key is not None:
                    raise Exception("Duplicate range key {}".format(field.name))
                self.range_key = field

        if not self.layout.is_complex and len(self.keys) > 1:
            raise Exception(
                "More than one key {} field in non complex layout {}".format(
                    len(self.keys), self.layout.name
                )
            )

        if self.layout.is_ranged and (
            not self.range_key or len(self.range_fields) != 2
        ):
            raise Exception("Inconsistent configuration of ranged dictionary")

    def get_structure_str(self):
        fields_strs = []
        for field in self.ordinary_fields:
            fields_strs.append(field.get_attribute_str())

        key_strs = []
        if self.layout.is_complex:
            for key_field in self.keys:
                key_strs.append(key_field.get_attribute_str())
        else:  # same for simple and ranged
            for key_field in self.keys:
                key_strs.append(key_field.get_simple_index_str())

        ranged_strs = []
        if self.layout.is_ranged:
            for range_field in self.range_fields:
                ranged_strs.append(range_field.get_range_hash_str())

        return """
        <layout>
            {layout_str}
        </layout>
        <structure>
            <{key_block_name}>
                {key_str}
            </{key_block_name}>
            {range_strs}
            {attributes_str}
        </structure>""".format(
            layout_str=self.layout.get_str(),
            key_block_name=self.layout.get_key_block_name(),
            key_str="\n".join(key_strs),
            attributes_str="\n".join(fields_strs),
            range_strs="\n".join(ranged_strs),
        )

    def get_ordered_names(self):
        fields_strs = []
        for key_field in self.keys:
            fields_strs.append(key_field.name)
        for range_field in self.range_fields:
            fields_strs.append(range_field.name)
        for field in self.ordinary_fields:
            fields_strs.append(field.name)
        return fields_strs

    def get_all_fields(self):
        return self.keys + self.range_fields + self.ordinary_fields

    def _get_dict_get_common_expression(
        self, dict_name, field, row, or_default, with_type, has
    ):
        if field in self.keys:
            raise Exception(
                "Trying to receive key field {} from dictionary".format(field.name)
            )

        if not self.layout.is_complex:
            if not or_default:
                key_expr = ", toUInt64({})".format(row.data[self.keys[0].name])
            else:
                key_expr = ", toUInt64({})".format(self.keys[0].default_value_for_get)
        else:
            key_exprs_strs = []
            for key in self.keys:
                if not or_default:
                    val = row.data[key.name]
                else:
                    val = key.default_value_for_get
                if isinstance(val, str):
                    val = "'" + val + "'"
                key_exprs_strs.append(
                    "to{type}({value})".format(type=key.field_type, value=val)
                )
            key_expr = ", tuple(" + ",".join(key_exprs_strs) + ")"

        date_expr = ""
        if self.layout.is_ranged:
            val = row.data[self.range_key.name]
            if isinstance(val, str):
                val = "'" + val + "'"
            val = "to{type}({val})".format(type=self.range_key.field_type, val=val)

            date_expr = ", " + val

            if or_default:
                raise Exception(
                    "Can create 'dictGetOrDefault' query for ranged dictionary"
                )

        if or_default:
            or_default_expr = "OrDefault"
            if field.default_value_for_get is None:
                raise Exception(
                    "Can create 'dictGetOrDefault' query for field {} without default_value_for_get".format(
                        field.name
                    )
                )

            val = field.default_value_for_get
            if isinstance(val, str):
                val = "'" + val + "'"
            default_value_for_get = ", to{type}({value})".format(
                type=field.field_type, value=val
            )
        else:
            or_default_expr = ""
            default_value_for_get = ""

        if with_type:
            field_type = field.field_type
        else:
            field_type = ""

        field_name = ", '" + field.name + "'"
        if has:
            what = "Has"
            field_type = ""
            or_default = ""
            field_name = ""
            date_expr = ""
            def_for_get = ""
        else:
            what = "Get"

        return "dict{what}{field_type}{or_default}('{dict_name}'{field_name}{key_expr}{date_expr}{def_for_get})".format(
            what=what,
            field_type=field_type,
            dict_name=dict_name,
            field_name=field_name,
            key_expr=key_expr,
            date_expr=date_expr,
            or_default=or_default_expr,
            def_for_get=default_value_for_get,
        )

    def get_get_expressions(self, dict_name, field, row):
        return [
            self._get_dict_get_common_expression(
                dict_name, field, row, or_default=False, with_type=False, has=False
            ),
            self._get_dict_get_common_expression(
                dict_name, field, row, or_default=False, with_type=True, has=False
            ),
        ]

    def get_get_or_default_expressions(self, dict_name, field, row):
        if not self.layout.is_ranged:
            return [
                self._get_dict_get_common_expression(
                    dict_name, field, row, or_default=True, with_type=False, has=False
                ),
                self._get_dict_get_common_expression(
                    dict_name, field, row, or_default=True, with_type=True, has=False
                ),
            ]
        return []

    def get_has_expressions(self, dict_name, field, row):
        if not self.layout.is_ranged:
            return [
                self._get_dict_get_common_expression(
                    dict_name, field, row, or_default=False, with_type=False, has=True
                )
            ]
        return []

    def get_hierarchical_expressions(self, dict_name, row):
        if self.layout.is_simple:
            key_expr = "toUInt64({})".format(row.data[self.keys[0].name])
            return [
                "dictGetHierarchy('{dict_name}', {key})".format(
                    dict_name=dict_name,
                    key=key_expr,
                ),
            ]

        return []

    def get_is_in_expressions(self, dict_name, row, parent_row):
        if self.layout.is_simple:
            child_key_expr = "toUInt64({})".format(row.data[self.keys[0].name])
            parent_key_expr = "toUInt64({})".format(parent_row.data[self.keys[0].name])
            return [
                "dictIsIn('{dict_name}', {child_key}, {parent_key})".format(
                    dict_name=dict_name,
                    child_key=child_key_expr,
                    parent_key=parent_key_expr,
                )
            ]

        return []


class Dictionary(object):
    def __init__(
        self,
        name,
        structure,
        source,
        config_path,
        table_name,
        fields,
        min_lifetime=3,
        max_lifetime=5,
    ):
        self.name = name
        self.structure = copy.deepcopy(structure)
        self.source = copy.deepcopy(source)
        self.config_path = config_path
        self.table_name = table_name
        self.fields = fields
        self.min_lifetime = min_lifetime
        self.max_lifetime = max_lifetime

    def generate_config(self):
        with open(self.config_path, "w") as result:
            if "direct" not in self.structure.layout.get_str():
                result.write(
                    """
                <clickhouse>
                <dictionary>
                    <lifetime>
                        <min>{min_lifetime}</min>
                        <max>{max_lifetime}</max>
                    </lifetime>
                    <name>{name}</name>
                    {structure}
                    <source>
                    {source}
                    </source>
                </dictionary>
                </clickhouse>
                """.format(
                        min_lifetime=self.min_lifetime,
                        max_lifetime=self.max_lifetime,
                        name=self.name,
                        structure=self.structure.get_structure_str(),
                        source=self.source.get_source_str(self.table_name),
                    )
                )
            else:
                result.write(
                    """
                <clickhouse>
                <dictionary>
                    <name>{name}</name>
                    {structure}
                    <source>
                    {source}
                    </source>
                </dictionary>
                </clickhouse>
                """.format(
                        min_lifetime=self.min_lifetime,
                        max_lifetime=self.max_lifetime,
                        name=self.name,
                        structure=self.structure.get_structure_str(),
                        source=self.source.get_source_str(self.table_name),
                    )
                )

    def prepare_source(self, cluster):
        self.source.prepare(self.structure, self.table_name, cluster)

    def load_data(self, data):
        if not self.source.prepared:
            raise Exception(
                "Cannot load data for dictionary {}, source is not prepared".format(
                    self.name
                )
            )

        self.source.load_data(data, self.table_name)

    def get_select_get_queries(self, field, row):
        return [
            "select {}".format(expr)
            for expr in self.structure.get_get_expressions(self.name, field, row)
        ]

    def get_select_get_or_default_queries(self, field, row):
        return [
            "select {}".format(expr)
            for expr in self.structure.get_get_or_default_expressions(
                self.name, field, row
            )
        ]

    def get_select_has_queries(self, field, row):
        return [
            "select {}".format(expr)
            for expr in self.structure.get_has_expressions(self.name, field, row)
        ]

    def get_hierarchical_queries(self, row):
        return [
            "select {}".format(expr)
            for expr in self.structure.get_hierarchical_expressions(self.name, row)
        ]

    def get_is_in_queries(self, row, parent_row):
        return [
            "select {}".format(expr)
            for expr in self.structure.get_is_in_expressions(self.name, row, parent_row)
        ]

    def is_complex(self):
        return self.structure.layout.is_complex

    def get_fields(self):
        return self.fields
