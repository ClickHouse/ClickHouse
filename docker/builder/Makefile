build:
	docker run --rm --workdir /server -v $(realpath ../..):/server -it yandex/clickhouse-builder

pull:
	docker pull yandex/clickhouse-builder

image:
	docker build -t yandex/clickhouse-builder .

image_push:
	docker push yandex/clickhouse-builder
