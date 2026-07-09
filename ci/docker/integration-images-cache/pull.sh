#!/bin/bash
# Fetches every image listed in $1 (one reference per line, # comments allowed)
# into docker-archive tarballs under $2 and writes $2/index.txt with the tar
# file names. Runs inside the clickhouse/clickhouse-integration-test image build,
# where no docker daemon is available, so skopeo fetches the images
# daemon-less.
#
# The pull set must match images.txt exactly: any pull failure fails the image
# build (the drift guard for the list itself is the `integration_images_cache`
# style check). The one exception is images with no manifest for the build
# architecture (e.g. amd64-only images on the arm64 build) - those are skipped,
# exactly as ci/jobs/scripts/prefetch-integration-test-images does at job time.
set -uo pipefail

list_file="$1"
out_dir="$2"
# Set by buildx; skopeo must fetch the image for the platform being built,
# not the build host's.
arch="${TARGETARCH:?TARGETARCH must be set}"

mkdir -p "$out_dir"
: > "$out_dir/index.txt"

# In CI the in-region dockerhub pull-through proxy avoids Docker Hub rate
# limits; local builds of this image cannot resolve it and go to docker.io.
mirror="dockerhub-proxy.dockerhub-proxy-zone:5000"
src_prefix="docker://docker.io/"
src_args=()
if timeout 5 curl -s -o /dev/null "http://$mirror/v2/"; then
    echo "Using dockerhub proxy $mirror"
    src_prefix="docker://$mirror/"
    src_args=(--src-tls-verify=false)
else
    echo "Dockerhub proxy $mirror unreachable, pulling from docker.io directly"
fi

failed=0
while read -r image; do
    [[ -z "$image" || "$image" == \#* ]] && continue
    # Official images need the explicit library/ namespace when a registry
    # host is spelled out.
    path="$image"
    [[ "$path" == */* ]] || path="library/$path"
    safe="${image//[^a-zA-Z0-9._-]/_}"
    tar_path="$out_dir/$safe.tar"

    echo "Pulling $image for $arch"
    out=""
    for attempt in 1 2 3; do
        if out=$(skopeo --override-os linux --override-arch "$arch" copy \
                "${src_args[@]}" "$src_prefix$path" \
                "docker-archive:$tar_path:$image" 2>&1); then
            echo "$safe.tar" >> "$out_dir/index.txt"
            du -h "$tar_path"
            continue 2
        fi
        # Arch-specific image - not a failure for this platform.
        if grep -q "no image found in manifest list" <<< "$out"; then
            echo "SKIP $image: no manifest for $arch"
            rm -f "$tar_path"
            continue 2
        fi
        echo "Pull of $image failed (attempt $attempt/3):"
        echo "$out"
        rm -f "$tar_path"
        sleep 5
    done
    echo "FAILED to pull $image"
    failed=1
done < "$list_file"

if ((failed)); then
    echo "ERROR: some images could not be pulled"
    exit 1
fi

echo "Preseed complete:"
sort "$out_dir/index.txt"
du -sh "$out_dir"
