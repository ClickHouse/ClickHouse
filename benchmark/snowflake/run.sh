#!/bin/bash

sed -r -e 's/^(.*)$/\1 \1 \1/' queries.sql | snowsql --region eu-central-1 --schemaname PUBLIC --dbname HITS --warehouse TEST
