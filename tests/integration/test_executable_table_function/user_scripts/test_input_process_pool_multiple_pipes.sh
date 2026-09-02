#!/bin/bash

read -t 250 -u 4 read_data_from_4_fd;
read -t 250 -u 3 read_data_from_3_fd;
read -t 250 read_data_from_0_df;

printf "3\n";
printf "Key from 4 fd $read_data_from_4_fd\n";
printf "Key from 3 fd $read_data_from_3_fd\n";
printf "Key from 0 fd $read_data_from_0_df\n";
