build:
	docker run --network=host --rm --workdir /server --volume $(realpath ../..):/server -it yandex/clickhouse-builder

pull:
	docker pull yandex/clickhouse-builder

image:
	docker build --network=host -t yandex/clickhouse-builder .

image_push:
	docker push yandex/clickhouse-builder
