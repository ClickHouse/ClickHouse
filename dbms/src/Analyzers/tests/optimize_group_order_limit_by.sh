#!/bin/sh

echo "SELECT number, materialize('abc') FROM (SELECT number, 10 AS b FROM system.numbers) GROUP BY number, toString(number + 1), number + number, 1, 2, 'Hello', b" | ./optimize_group_order_limit_by
echo
echo "SELECT number FROM system.numbers GROUP BY 1 ORDER BY number, 'hello' DESC COLLATE 'tr', number + 1, rand(), identity(number * 2, rand()), toString(rand()) COLLATE 'tr'" | ./optimize_group_order_limit_by
