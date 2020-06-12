build: image
	mkdir -p $(HOME)/.ccache
	docker run --network=host --rm --workdir /server --volume $(realpath ../..):/server --cap-add=SYS_PTRACE --mount=type=bind,source=$(HOME)/.ccache,destination=/ccache -e CCACHE_DIR=/ccache -it yandex/clickhouse-builder

pull:
	docker pull yandex/clickhouse-builder

image:
	docker build --network=host -t yandex/clickhouse-builder .

image_push:
	docker push yandex/clickhouse-builder
