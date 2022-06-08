#!/bin/bash

test_filename="gtest_mysql_parser_stress.cpp"

rm -rf $test_filename

cat head.h >> $test_filename
python3 gen_tests.py >> $test_filename
cat test.cpp >> $test_filename
