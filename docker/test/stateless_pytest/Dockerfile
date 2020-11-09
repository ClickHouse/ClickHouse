# docker build -t yandex/clickhouse-stateless-pytest .
FROM yandex/clickhouse-deb-builder

RUN apt-get update -y && \
    apt-get install -y --no-install-recommends \
        python-pip \
        python-setuptools

RUN pip install \
    pytest \
    pytest-html \
    pytest-timeout \
    pytest-xdist

CMD dpkg -i package_folder/clickhouse-common-static_*.deb; \
    dpkg -i package_folder/clickhouse-common-static-dbg_*.deb; \
    dpkg -i package_folder/clickhouse-server_*.deb;  \
    dpkg -i package_folder/clickhouse-client_*.deb; \
    dpkg -i package_folder/clickhouse-test_*.deb; \
    python -m pytest /usr/share/clickhouse-test/queries -n $(nproc) --html=test_output/report.html --self-contained-html
