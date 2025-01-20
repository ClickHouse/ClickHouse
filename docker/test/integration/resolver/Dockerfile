# docker build -t altinityinfra/python-bottle .
# Helper docker container to run python bottle apps

FROM python:3
COPY requirements.txt /
RUN python -m pip install --no-cache-dir -r requirements.txt
