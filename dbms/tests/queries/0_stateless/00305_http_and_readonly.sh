#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# При POST можно делать что угодно.
curl -sS "http://localhost:8123/?query=SELECT+name,value,changed+FROM+system.settings+WHERE+name+IN+('readonly','max_rows_to_read')&max_rows_to_read=10000&default_format=PrettySpaceNoEscapes" -d' '

# При GET выставляется readonly = 2.
curl -sS "http://localhost:8123/?query=SELECT+name,value,changed+FROM+system.settings+WHERE+name+IN+('readonly','max_rows_to_read')&max_rows_to_read=10000&default_format=PrettySpaceNoEscapes"

# Можно самому усилить readonly и при этом изменить какие-то ещё настройки.
curl -sS "http://localhost:8123/?query=SELECT+name,value,changed+FROM+system.settings+WHERE+name+IN+('readonly','max_rows_to_read')&readonly=1&max_rows_to_read=10000&default_format=PrettySpaceNoEscapes" -d' '
curl -sS "http://localhost:8123/?query=SELECT+name,value,changed+FROM+system.settings+WHERE+name+IN+('readonly','max_rows_to_read')&readonly=2&max_rows_to_read=10000&default_format=PrettySpaceNoEscapes" -d' '

curl -vsS "http://localhost:8123/?query=DROP+TABLE+IF+EXISTS+test.nonexistent" 2>&1 | grep -q '500 Internal Server Error' && echo 'Ok' || echo 'Fail'
curl -vsS "http://localhost:8123/?readonly=0&query=DROP+TABLE+IF+EXISTS+test.nonexistent" 2>&1 | grep -q '500 Internal Server Error' && echo 'Ok' || echo 'Fail'

curl -sS "http://localhost:8123/?query=DROP+TABLE+IF+EXISTS+test.nonexistent" -d ' ' | wc -l
curl -sS "http://localhost:8123/?readonly=0&query=DROP+TABLE+IF+EXISTS+test.nonexistent" -d ' ' | wc -l

curl -vsS "http://localhost:8123/?readonly=1&query=DROP+TABLE+IF+EXISTS+test.nonexistent" -d ' ' 2>&1 | grep -q '500 Internal Server Error' && echo 'Ok' || echo 'Fail'
curl -vsS "http://localhost:8123/?readonly=2&query=DROP+TABLE+IF+EXISTS+test.nonexistent" -d ' ' 2>&1 | grep -q '500 Internal Server Error' && echo 'Ok' || echo 'Fail'
