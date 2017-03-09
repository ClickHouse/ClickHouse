#!/usr/bin/env bash

clickhouse-client --query="SELECT * FROM system.build_options" | perl -lnE 'print $1 if /(BUILD_DATE|BUILD_TYPE|CXX_COMPILER|CXX_FLAGS|LINK_FLAGS)\s+\S+/';
