#!/bin/sh -e

OS=$(uname -s)
ARCH=$(uname -m)

DIR=

if [ "${OS}" = "Linux" ]
then
    if [ "${ARCH}" = "x86_64" -o "${ARCH}" = "amd64" ]
    then
        # Require at least x86-64 + SSE4.2 (introduced in 2006). On older hardware fall back to plain x86-64 (introduced in 1999) which
        # guarantees at least SSE2. The caveat is that plain x86-64 builds are much less tested than SSE 4.2 builds.
        HAS_SSE42=$(grep sse4_2 /proc/cpuinfo)
        if [ "${HAS_SSE42}" ]
        then
            if ldd --version 2>&1 | grep -q musl
            then
                DIR="amd64musl"
            else
                DIR="amd64"
            fi
        else
            DIR="amd64compat"
        fi
    elif [ "${ARCH}" = "aarch64" -o "${ARCH}" = "arm64" ]
    then
        # Dispatch between standard and compatibility builds, see cmake/cpu_features.cmake for details. Unfortunately, (1) the ARM ISA level
        # cannot be read directly, we need to guess from the "features" in /proc/cpuinfo, and (2) the flags in /proc/cpuinfo are named
        # differently than the flags passed to the compiler in cpu_features.cmake.
        HAS_ARMV82=$(grep -m 1 'Features' /proc/cpuinfo | awk '/asimd/ && /sha1/ && /aes/ && /atomics/ && /lrcpc/')
        if [ "${HAS_ARMV82}" ]
        then
            DIR="aarch64"
        else
            DIR="aarch64v80compat"
        fi
    elif [ "${ARCH}" = "powerpc64le" -o "${ARCH}" = "ppc64le" ]
    then
        DIR="powerpc64le"
    elif [ "${ARCH}" = "riscv64" ]
    then
        DIR="riscv64"
    elif [ "${ARCH}" = "s390x" ]
    then
        DIR="s390x"
    fi
elif [ "${OS}" = "FreeBSD" ]
then
    if [ "${ARCH}" = "x86_64" -o "${ARCH}" = "amd64" ]
    then
        DIR="freebsd"
    fi
elif [ "${OS}" = "Darwin" ]
then
    if [ "${ARCH}" = "x86_64" -o "${ARCH}" = "amd64" ]
    then
        DIR="macos"
    elif [ "${ARCH}" = "aarch64" -o "${ARCH}" = "arm64" ]
    then
        DIR="macos-aarch64"
    fi
fi

if [ -z "${DIR}" ]
then
    echo "Operating system '${OS}' / architecture '${ARCH}' is unsupported."
    exit 1
fi

clickhouse_download_filename_prefix="clickhouse"
clickhouse="$clickhouse_download_filename_prefix"

if [ -f "$clickhouse" ]
then
    read -p "ClickHouse binary ${clickhouse} already exists. Overwrite? [y/N] " answer
    if [ "$answer" = "y" -o "$answer" = "Y" ]
    then
        rm -f "$clickhouse"
    else
        i=0
        while [ -f "$clickhouse" ]
        do
            clickhouse="${clickhouse_download_filename_prefix}.${i}"
            i=$(($i+1))
        done
    fi
fi

URL="https://builds.clickhouse.com/master/${DIR}/clickhouse"
echo
echo "Will download ${URL} into ${clickhouse}"
echo
curl "${URL}" -o "${clickhouse}" && chmod a+x "${clickhouse}" || exit 1
echo
echo "Successfully downloaded the ClickHouse binary, you can run it as:
    ./${clickhouse}"

echo
echo "You can also install it:
sudo ./${clickhouse} install"

# Also install clickhousectl, the CLI for ClickHouse local and Cloud
chctl_target=
if [ "${OS}" = "Linux" ]
then
    if [ "${ARCH}" = "x86_64" -o "${ARCH}" = "amd64" ]
    then
        chctl_target="x86_64-unknown-linux-musl"
    elif [ "${ARCH}" = "aarch64" -o "${ARCH}" = "arm64" ]
    then
        chctl_target="aarch64-unknown-linux-musl"
    fi
elif [ "${OS}" = "Darwin" ]
then
    if [ "${ARCH}" = "x86_64" -o "${ARCH}" = "amd64" ]
    then
        chctl_target="x86_64-apple-darwin"
    elif [ "${ARCH}" = "aarch64" -o "${ARCH}" = "arm64" ]
    then
        chctl_target="aarch64-apple-darwin"
    fi
fi

if [ -n "${chctl_target}" ]
then
    echo
    echo "Fetching the latest clickhousectl release..."
    chctl_tag=$(curl -fsSL "https://api.github.com/repos/ClickHouse/clickhousectl/releases/latest" \
        | grep '"tag_name"' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')

    if [ -n "${chctl_tag}" ]
    then
        chctl_install_dir="${HOME}/.local/bin"
        chctl_archive="clickhousectl-${chctl_target}-${chctl_tag}.tar.gz"
        chctl_url="https://github.com/ClickHouse/clickhousectl/releases/download/${chctl_tag}/${chctl_archive}"
        echo "Will download ${chctl_url} into ${chctl_install_dir}/clickhousectl"
        chctl_tmp=$(mktemp -d)
        if mkdir -p "${chctl_install_dir}" \
            && curl -fsSL "${chctl_url}" -o "${chctl_tmp}/${chctl_archive}" \
            && tar -xzf "${chctl_tmp}/${chctl_archive}" -C "${chctl_tmp}" \
            && mv -f "${chctl_tmp}/clickhousectl-${chctl_target}-${chctl_tag}/clickhousectl" "${chctl_install_dir}/clickhousectl"
        then
            chmod a+x "${chctl_install_dir}/clickhousectl"
            ln -sf "${chctl_install_dir}/clickhousectl" "${chctl_install_dir}/chctl"
            echo
            echo "Successfully installed clickhousectl to ${chctl_install_dir}/clickhousectl"
            echo "Created alias: chctl -> clickhousectl"
            case ":$PATH:" in
                *":${chctl_install_dir}:"*) ;;
                *)
                    echo
                    echo "NOTE: ${chctl_install_dir} is not in your PATH."
                    echo "Add it by running:"
                    echo
                    echo "  export PATH=\"${chctl_install_dir}:\$PATH\""
                    echo
                    echo "You may want to add that line to your shell profile (~/.bashrc, ~/.zshrc, etc.)"
                    ;;
            esac
        else
            echo "Warning: failed to download clickhousectl. Continuing."
        fi
        rm -rf "${chctl_tmp}"
    else
        echo "Warning: could not determine the latest clickhousectl release. Continuing."
    fi
fi
