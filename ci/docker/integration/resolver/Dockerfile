# docker build -t clickhouse/python-bottle .
# Helper docker container to run python bottle apps

# python cgi module is dropped in 3.13 - pin to 3.12
FROM python:3.12
COPY requirements.txt /
RUN python -m pip install --no-cache-dir -r requirements.txt
