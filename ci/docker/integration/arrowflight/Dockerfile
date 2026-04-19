# docker build -t clickhouse/arrowflight-server-test .
FROM python:3.9-slim

ENV PYTHONDONTWRITEBYTECODE=1

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY flight_server.py /app/
COPY run.sh /app/

RUN pip install --no-cache-dir pyarrow

EXPOSE 5005 5006

CMD ["/bin/bash", "run.sh"]
