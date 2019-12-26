#!/bin/bash

cd /workspace

../compare.sh $LEFT_PR $LEFT_SHA $RIGHT_PR $RIGHT_SHA > compare.log 2>&1

7z a /output/output.7z *.log *.tsv
cp compare.log /output
