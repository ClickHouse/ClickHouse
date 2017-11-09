#!/usr/bin/env bash

curl -vsS http://localhost:8123/ --data-binary @- <<< "SELECT 1" 2>&1 | perl -lnE 'print if /Keep-Alive/';
curl -vsS http://localhost:8123/ --data-binary @- <<< " error here " 2>&1 | perl -lnE 'print if /Keep-Alive/';
curl -vsS http://localhost:8123/ping  2>&1 | perl -lnE 'print if /Keep-Alive/';
curl -vsS http://localhost:8123/replicas_status 2>&1 | perl -lnE 'print if /Keep-Alive/';

# no keep-alive:
curl -vsS http://localhost:8123/404/not/found/ 2>&1 | perl -lnE 'print if /Keep-Alive/';
