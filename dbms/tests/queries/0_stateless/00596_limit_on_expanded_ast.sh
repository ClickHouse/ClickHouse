#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

exception_pattern="too big"

${CLICKHOUSE_CLIENT} --max_expanded_ast_elements=500000 --query="
    select 1 as a, a+a as b, b+b as c, c+c as d, d+d as e, e+e as f, f+f as g, g+g as h, h+h as i, i+i as j, j+j as k, k+k as l, l+l as m, m+m as n, n+n as o, o+o as p, p+p as q, q+q as r, r+r as s, s+s as t, t+t as u, u+u as v, v+v as w, w+w as x, x+x as y, y+y as z
" 2>&1 | grep -c "$exception_pattern"
