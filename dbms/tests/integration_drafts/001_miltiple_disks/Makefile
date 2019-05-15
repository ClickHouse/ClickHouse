docker_compose_version := $(shell echo $$(uname -s)-$$(uname -m))

run:
	docker-compose up -d

install_docker:
	@echo ""
	@echo "Please remember that you need to call it with sudo!!"
	@echo "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
	@echo ""
	-curl -sSL https://get.docker.com | bash
	-usermod -aG docker $${USER}
	@echo "$${USER} has been added to docker group. Please relogin"

install_docker_compose:
	@echo ""
	@echo "Please remember that you need to call it with sudo!!"
	@echo "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
	@echo ""
	-curl -L "https://github.com/docker/compose/releases/download/1.23.2/docker-compose-$(docker_compose_version)" -o /usr/local/bin/docker-compose
	-chmod +x /usr/local/bin/docker-compose

ch1_shell:
	docker-compose exec ch1 bash

ch2_shell:
	docker-compose exec ch2 bash

stop:
	docker-compose down

logs:
	docker-compose logs -f

cleanup:
	docker-compose down --rmi local --remove-orphans -v

cleanup_all:
	docker container stop $$(docker container  ls -a -q)
	docker system prune --volumes
	docker image prune --all

