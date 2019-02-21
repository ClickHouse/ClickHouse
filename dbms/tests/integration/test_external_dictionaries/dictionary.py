#-*- coding: utf-8 -*-


class Layout(object):
    LAYOUTS_STR_DICT = {
        'flat': '<flat/>',
        'hashed': '<hashed/>',
        'cache': '<cache><size_in_cells>128</size_in_cells></cache>',
        'complex_key_hashed': '<complex_key_hashed/>',
        'complex_key_cache': '<complex_key_cache><size_in_cells>128</size_in_cells></complex_key_cache>',
        'range_hashed': '<range_hashed/>'
    }

    def __init__(self, name):
        self.name = name
        self.is_complex = False
        self.is_simple = False
        self.is_ranged = False
        if self.name.startswith('complex'):
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
            return 'key'
        else:
            return 'id'


class Row(object):
    def __init__(self, fields, values):
        self.data = {}
        for field, value in zip(fields, values):
            self.data[field.name] = value

    def get_value_by_name(self, name):
        return self.data[name]


class Field(object):
    def __init__(self, name, field_type, is_key=False, is_range_key=False, default=None, hierarchical=False, range_hash_type=None):
        self.name = name
        self.field_type = field_type
        self.is_key = is_key
        self.default = default
        self.hierarchical = hierarchical
        self.range_hash_type = range_hash_type
        self.is_range = self.range_hash_type is not None
        self.is_range_key = is_range_key

    def get_attribute_str(self):
        return '''
            <attribute>
              <name>{name}</name>
              <type>{field_type}</type>
              <null_value>{default}</null_value>
              <hierarchical>{hierarchical}</hierarchical>
            </attribute>'''.format(
            name=self.name,
            field_type=self.field_type,
            default=self.default if self.default else '',
            hierarchical='true' if self.hierarchical else 'false',
        )

    def get_simple_index_str(self):
        return '<name>{name}</name>'.format(name=self.name)

    def get_range_hash_str(self):
        if not self.range_hash_type:
            raise Exception("Field {} is not range hashed".format(self.name))
        return '''
            <range_{type}>
                <name>{name}</name>
            </range_{type}>
        '''.format(type=self.range_hash_type, name=self.name)


class DictionaryStructure(object):
    def __init__(self, layout, fields):
        self.layout = layout
        self.keys = []
        self.range_key = None
        self.ordinary_fields = []
        self.range_fields = []
        for field in fields:
            if field.is_key:
                self.keys.append(field)
            elif field.is_range:
                self.range_fields.append(field)
            else:
                self.ordinary_fields.append(field)

            if field.is_range_key:
                if self.range_key is not None:
                    raise Exception("Duplicate range key {}".format(field.name))
                self.range_key = field

        if not self.layout.is_complex and len(self.keys) > 1:
            raise Exception("More than one key {} field in non complex layout {}".format(len(self.keys), self.layout.name))

        if self.layout.is_ranged and (not self.range_key or len(self.range_fields) != 2):
            raise Exception("Inconsistent configuration of ranged dictionary")

    def get_structure_str(self):
        fields_strs = []
        for field in self.ordinary_fields:
            fields_strs.append(field.get_attribute_str())
        key_strs = []
        if self.layout.is_complex:
            for key_field in self.keys:
                key_strs.append(key_field.get_attribute_str())
        else: # same for simple and ranged
            for key_field in self.keys:
                key_strs.append(key_field.get_simple_index_str())

        ranged_strs = []
        if self.layout.is_ranged:
            for range_field in self.range_fields:
                ranged_strs.append(range_field.get_range_hash_str())

        return '''
        <layout>
            {layout_str}
        </layout>
        <structure>
            <{key_block_name}>
                {key_str}
            </{key_block_name}>
            {attributes_str}
            {range_strs}
        </structure>'''.format(
            layout_str=self.layout.get_str(),
            key_block_name=self.layout.get_key_block_name(),
            key_str='\n'.join(key_strs),
            attributes_str='\n'.join(fields_strs),
            range_strs='\n'.join(ranged_strs),
        )

    def get_dict_get_expression(self, dict_name, field, row):
        if field in self.keys:
            raise Exception("Trying to receive key field {} from dictionary".format(field.name))

        if not self.layout.is_complex:
            key_expr = 'toUInt64({})'.format(row.data[self.keys[0].name])
        else:
            key_exprs_strs = []
            for key in self.keys:
                val = row.data[key.name]
                if isinstance(val, str):
                    val = "'" + val + "'"
                key_exprs_strs.append('to{type}({value})'.format(type=key.field_type, value=val))
            key_expr = '(' + ','.join(key_exprs_strs) + ')'

        date_expr = ''
        if self.layout.is_ranged:
            val = row.data[self.range_key.name]
            if isinstance(val, str):
                val = "'" + val + "'"
            val = "to{type}({val})".format(type=self.range_key.field_type, val=val)

            date_expr = ', ' + val

        return "dictGet{field_type}('{dict_name}', '{field_name}', {key_expr}{date_expr})".format(
            field_type=field.field_type,
            dict_name=dict_name,
            field_name=field.name,
            key_expr=key_expr,
            date_expr=date_expr,
        )


class Dictionary(object):
    def __init__(self, name, structure, source, config_path, table_name):
        self.name = name
        self.structure = structure
        self.source = source
        self.config_path = config_path
        self.table_name = table_name

    def generate_config(self):
        with open(self.config_path, 'w') as result:
            result.write('''
            <dictionaries>
                <dictionary>
                    <lifetime>
                        <min>3</min>
                        <max>5</max>
                    </lifetime>
                    <name>{name}</name>
                    {structure}
                    <source>
                    {source}
                    </source>
                </dictionary>
            </dictionaries>
            '''.format(
                name=self.name,
                structure=self.structure.get_structure_str(),
                source=self.source.get_source_str(self.table_name),
            ))

    def prepare_source(self):
        self.source.prepare(self.structure, self.table_name)

    def load_data(self, data):
        if not self.source.prepared:
            raise Exception("Cannot load data for dictionary {}, source is not prepared".format(self.name))

        self.source.load_data(data, self.table_name)

    def get_select_query(self, field, row):
        return 'select {}'.format(self.structure.get_dict_get_expression(self.name, field, row))

    def is_complex(self):
        return self.structure.layout.is_complex
