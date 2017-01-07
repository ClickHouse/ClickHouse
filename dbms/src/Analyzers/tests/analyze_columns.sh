#!/bin/sh

echo "SELECT dummy, number, one.dummy, numbers.number, system.one.dummy, system.numbers.number, one.*, numbers.*, system.one.*, system.numbers.*, *, t.*, t.number FROM system.one, system.numbers AS t" | ./analyze_columns
