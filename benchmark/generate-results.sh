#!/bin/bash

ls -1 */results/*.json | while read file
do
    cat "${file}"
    echo ','
done
