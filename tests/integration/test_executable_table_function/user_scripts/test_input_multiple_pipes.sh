#!/bin/bash

while read -t 250 -u 4 read_data; do printf "Key from 4 fd $read_data\n"; done
while read -t 250 -u 3 read_data; do printf "Key from 3 fd $read_data\n"; done
while read -t 250 read_data; do printf "Key from 0 fd $read_data\n"; done
