#!/usr/bin/env bash

curl -s --insecure 'https://localhost:8443/' -d 'SELECT number FROM system.numbers LIMIT 1'
