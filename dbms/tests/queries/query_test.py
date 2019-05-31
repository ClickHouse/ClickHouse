import pytest

import difflib
import os
import subprocess
import sys


@pytest.mark.timeout(timeout=10, method='signal')
def test_query(bin_prefix, sql_query, standalone_server):
    tcp_port = standalone_server.tcp_port

    query_path = sql_query + ".sql"
    reference_path = sql_query + ".reference"

    if not os.path.exists(reference_path):
        pytest.skip('No .reference file found')

    with open(query_path, 'r') as file:
        query = file.read()
    with open(reference_path, 'r') as file:
        reference = file.read()

    client = subprocess.Popen([bin_prefix + '-client', '--port', str(tcp_port), '-m', '-n', '--testmode'],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result, error = client.communicate(query)
    assert client.returncode is not None, "Client should exit after processing all queries"

    if client.returncode != 0:
        print >> sys.stderr, error
        pytest.fail('Client died unexpectedly with code {code}'.format(code=client.returncode), pytrace=False)
    elif result != reference:
        pytest.fail("Query output doesn't match reference:{eol}{diff}".format(
                eol=os.linesep,
                diff=os.linesep.join(l.strip() for l in difflib.unified_diff(reference.splitlines(), result.splitlines(), fromfile='expected', tofile='actual'))),
            pytrace=False)
