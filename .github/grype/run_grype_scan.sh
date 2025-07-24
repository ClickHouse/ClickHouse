set -x
set -e

IMAGE=$1

GRYPE_VERSION=${GRYPE_VERSION:-"v0.92.2"}

docker pull $IMAGE
docker pull anchore/grype:${GRYPE_VERSION}

docker run \
 --rm --volume /var/run/docker.sock:/var/run/docker.sock \
 --name Grype anchore/grype:${GRYPE_VERSION} \
 --scope all-layers \
 -o json \
 $IMAGE > result.json

ls -sh
