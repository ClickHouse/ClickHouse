#!/usr/bin/env python

import sys
import json

def prepare_comparison(test_results):
    queries = []
    query_to_version_to_results = {}
    version_to_time = {}

    for test in test_results:
        version = test['server_version']
        version_to_time[version] = test['time']

        for run in test['runs']:
            query = run['query']
            version_to_results = query_to_version_to_results.setdefault(query, {})

            if len(version_to_results) == 0:
                queries.append(query)

            version_to_results.setdefault(version, []).append(run['min_time'])

    results = []
    for version in sorted(list(version_to_time.keys())):
        result = [query_to_version_to_results[q].get(version, []) for q in queries]

        results.append({
            'system': 'ClickHouse',
            'version': version,
            'data_size': 100000000,
            'time': version_to_time[version],
            'comments': '',
            'result': result,
            })

    queries = [{'query': q, 'comment': ''} for q in queries]

    return queries, results

if __name__ == '__main__':
    json_files = sys.argv[1:]

    queries, results = prepare_comparison(sum([json.load(open(f)) for f in json_files], []))

    print 'var queries ='
    print json.dumps(queries, indent=4, separators=(',', ': ')), ';'

    print 'var results ='
    print json.dumps(results, indent=4, separators=(',', ': ')), ';'
