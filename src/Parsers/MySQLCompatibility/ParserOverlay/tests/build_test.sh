#!/bin/bash

rm -rf gtest_second.cpp

cat head.txt >> gtest_second.cpp
python3 gen_tests.py >> gtest_second.cpp
cat end.txt >> gtest_second.cpp
