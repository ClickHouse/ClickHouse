## ClickHouse Dockerfiles

This directory contain Dockerfiles for `clickhouse-server`. They are updated in each release.

Also, there is a bunch of images for testing and CI. They are listed in `images.json` file and updated on each commit to master. If you need to add another image, place information about it into `images.json`.

### How to add a new image to the hub.docker.com
- Login to the docker hub with a user having admin rights in `clickhouse` organization (search the company secrets storage by `docker`)
- Create an image with the necessary name
- Add `Read and Write` permissions for a group `write` to it
- Optionally, add a description
