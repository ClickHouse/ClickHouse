#!/usr/bin/env bash

echo -ne '1,Hello\n2,World\n' | curl -sSF 'file=@-' 'http://localhost:8123/?query=SELECT+*+FROM+file&file_format=CSV&file_types=UInt8,String';
