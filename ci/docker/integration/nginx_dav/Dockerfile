FROM nginx:alpine-slim

COPY default.conf /etc/nginx/conf.d/

RUN mkdir /usr/share/nginx/files/ \
    && chown nginx: /usr/share/nginx/files/ -R
