#!/usr/bin/python3

import itertools
import clickhouse_driver
import xml.etree.ElementTree as et
import argparse
import pprint

parser = argparse.ArgumentParser(description='Run performance test.')
parser.add_argument('file', metavar='FILE', type=argparse.FileType('r'), nargs=1, help='test description file')
args = parser.parse_args()

tree = et.parse(args.file[0])
root = tree.getroot()

# Check main metric
main_metric = root.find('main_metric/*').tag
if main_metric != 'min_time':
    raise Exception('Only the min_time main metric is supported. This test uses \'{}\''.format(main_metric))

# Open connections
servers = [{'host': 'localhost', 'port': 9000, 'client_name': 'left'}, {'host': 'localhost', 'port': 9001, 'client_name': 'right'}]
connections = [clickhouse_driver.Client(**server) for server in servers]

# Check tables that should exist
tables = [e.text for e in root.findall('preconditions/table_exists')]
if tables:
    for c in connections:
        tables_list = ", ".join("'{}'".format(t) for t in tables)
        res = c.execute("select t from values('t text', {}) anti join system.tables on database = currentDatabase() and name = t".format(tables_list))
        if res:
            raise Exception('Some tables are not found: {}'.format(res))

# Process substitutions
subst_elems = root.findall('substitutions/substitution')

parameter_keys = []         # ['table',                   'limit'    ]
parameter_value_arrays = [] # [['hits_100m', 'hits_10m'], ['1', '10']]
parameter_combinations = [] # [{table: hits_100m, limit: 1}, ...]
for se in subst_elems:
    parameter_keys.append(se.find('name').text)
    parameter_value_arrays.append([v.text for v in se.findall('values/value')])
parameter_combinations = [dict(zip(parameter_keys, parameter_combination)) for parameter_combination in itertools.product(*parameter_value_arrays)]

def substitute_parameters(query_templates, parameter_combinations):
    return list(set([template.format(**parameters) for template, parameters in itertools.product(query_templates, parameter_combinations)]))

# Run drop queries, ignoring errors
drop_query_templates = [q.text for q in root.findall('drop_query')]
drop_queries = substitute_parameters(drop_query_templates, parameter_combinations)
for c in connections:
    for q in drop_queries:
        try:
            c.execute(q)
        except:
            print("Error:", sys.exc_info()[0], file=sys.stderr)

# Run create queries
create_query_templates = [q.text for q in root.findall('create_query')]
create_queries = substitute_parameters(create_query_templates, parameter_combinations)
for c in connections:
    for q in create_queries:
        c.execute(q)

# Run fill queries
fill_query_templates = [q.text for q in root.findall('fill_query')]
fill_queries = substitute_parameters(fill_query_templates, parameter_combinations)
for c in connections:
    for q in fill_queries:
        c.execute(q)

# Run test queries
test_query_templates = [q.text for q in root.findall('query')]
test_queries = substitute_parameters(test_query_templates, parameter_combinations)

for q in test_queries:
    for run in range(0, 7):
        for conn_index, c in enumerate(connections):
            res = c.execute(q)
            print(q + '\t' + str(run) + '\t' + str(conn_index) + '\t' + str(c.last_query.elapsed))

# Run drop queries
drop_query_templates = [q.text for q in root.findall('drop_query')]
drop_queries = substitute_parameters(drop_query_templates, parameter_combinations)
for c in connections:
    for q in drop_queries:
        c.execute(q)
