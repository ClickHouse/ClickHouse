# docker build -t altinityinfra/mysql-js-client .
# MySQL JavaScript client docker container
# Rebuild to enable SBOM/provenance attestations (see #97903, #98511)

FROM node:22-alpine

WORKDIR /usr/app

# NOTE (strtgbb): removed npm upgrade step to avoid a dependency issue
RUN npm install mysql

COPY ./test.js ./test.js
