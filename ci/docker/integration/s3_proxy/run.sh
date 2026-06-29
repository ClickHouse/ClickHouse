#!/usr/bin/env sh

if [ -z "$S3_HOST" ] ; then
    S3_HOST='minio1'
fi

if [ -z "$S3_PORT" ] ; then
    S3_PORT='9001'
fi

# Replace config placeholders with environment variables
sed -i "s|\${S3_HOST}|${S3_HOST}|" /etc/nginx/nginx.conf
sed -i "s|\${S3_PORT}|${S3_PORT}|" /etc/nginx/nginx.conf

exec nginx -g 'daemon off;'
