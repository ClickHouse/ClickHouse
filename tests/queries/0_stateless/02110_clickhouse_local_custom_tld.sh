#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function clickhouse_local()
{
    local opts=(
        --config "$CURDIR/$(basename "${BASH_SOURCE[0]}" .sh).config.xml"
        --top_level_domains_path "$CURDIR"
    )
    $CLICKHOUSE_LOCAL "${opts[@]}" "$@"
}

# -- biz.ss is not in the default TLD list, hence:
clickhouse_local -q "
    select
        cutToFirstSignificantSubdomain('foo.kernel.biz.ss'),
        cutToFirstSignificantSubdomainCustom('foo.kernel.biz.ss', 'public_suffix_list')
" |& grep -v -e 'Processing configuration file'
