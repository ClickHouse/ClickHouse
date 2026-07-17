#!/bin/sh -e

# Decorated output is enabled only where it will render correctly:
# colors need stdout to be a TTY and a capable TERM (the script itself arrives
# on stdin when run as `curl https://clickhouse.com/ | sh`, but stdout is still
# the terminal there), and the logo needs a UTF-8 locale.
# NO_COLOR (https://no-color.org) disables colors.
ESC=$(printf '\033')
RESET=''; BOLD=''; DIM=''; YELLOW=''; CYAN=''; BLUE=''
if [ -t 1 ] && [ -n "${TERM}" ] && [ "${TERM}" != "dumb" ] && [ -z "${NO_COLOR}" ]
then
    RESET="${ESC}[0m"; BOLD="${ESC}[1m"; DIM="${ESC}[2m"
    YELLOW="${ESC}[93m"; CYAN="${ESC}[36m"; BLUE="${ESC}[34m"
fi

echo
case "${LC_ALL:-${LC_CTYPE:-${LANG:-}}}" in
    *[Uu][Tt][Ff]-8*|*[Uu][Tt][Ff]8*)
        printf '%s\n' "   ${BOLD}█ █ █ █${RESET}"
        printf '%s\n' "   ${BOLD}█ █ █ █ ▄${RESET}     ${BOLD}ClickHouse${RESET}"
        printf '%s\n' "   ${BOLD}█ █ █ █ ▀${RESET}     ${DIM}the fastest open source database${RESET}"
        printf '%s\n' "   ${BOLD}█ █ █ █${RESET}       ${DIM}for real-time analytics${RESET}"
        ;;
    *)
        printf '%s\n' "   ${BOLD}ClickHouse${RESET} ${DIM}- the fastest open source database for real-time analytics${RESET}"
        ;;
esac
echo

OS=$(uname -s)
ARCH=$(uname -m)

DIR=

if [ "${OS}" = "Linux" ]
then
    if [ "${ARCH}" = "x86_64" -o "${ARCH}" = "amd64" ]
    then
        # The default build targets x86-64-v3 which requires AVX2, BMI1, BMI2, FMA, etc.
        # On older hardware, fall back to the compat build (plain x86-64, SSE2 baseline).
        # Check avx2 as a proxy, since every real CPU with AVX2 also has the other v3 features.
        if grep -q avx2 /proc/cpuinfo
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

printf '%s\n' "${BLUE}==>${RESET} ${BOLD}Detecting platform${RESET}"

if [ -z "${DIR}" ]
then
    echo "    Operating system '${OS}' / architecture '${ARCH}' is unsupported." >&2
    exit 1
fi

printf '%s\n' "    ${OS} ${ARCH} -> ${DIR} (latest master build)"

clickhouse_download_filename_prefix="clickhouse"
clickhouse="$clickhouse_download_filename_prefix"

# If something already exists at this path, pick a non-clashing name (clickhouse.0, clickhouse.1, ...).
# Do not prompt interactively here: this script is commonly run as `curl https://clickhouse.com/ | sh`,
# where the script itself is delivered on stdin. A `read` would consume bytes from that same pipe,
# desyncing the shell parser and producing spurious syntax errors.
# Use `-e` together with `-L` so that directories and broken symlinks are also treated as occupied
# (a dangling symlink is invisible to `-e` alone, and `curl -o` would then fail on it).
i=0
while [ -e "$clickhouse" ] || [ -L "$clickhouse" ]
do
    clickhouse="${clickhouse_download_filename_prefix}.${i}"
    i=$(($i+1))
done

URL="https://builds.clickhouse.com/master/${DIR}/clickhouse"
printf '%s\n' "${BLUE}==>${RESET} ${BOLD}Downloading clickhouse${RESET}"
printf '%s\n' "    ${DIM}${URL} -> ./${clickhouse}${RESET}"
curl "${URL}" -o "${clickhouse}" && chmod a+x "${clickhouse}" || exit 1

# Also install clickhousectl, the CLI for ClickHouse local and Cloud.
# Set CLICKHOUSE_ONLY=1 to skip.
chctl_installed=
if [ -z "${CLICKHOUSE_ONLY}" ]
then
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
        chctl_tag=$(curl -fsSL "https://api.github.com/repos/ClickHouse/clickhousectl/releases/latest" \
            | grep '"tag_name"' | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')

        if [ -n "${chctl_tag}" ]
        then
            chctl_install_dir="${HOME}/.local/bin"
            chctl_archive="clickhousectl-${chctl_target}-${chctl_tag}.tar.gz"
            chctl_url="https://builds.clickhouse.com/clickhousectl/${chctl_archive}"
            printf '%s\n' "${BLUE}==>${RESET} ${BOLD}Downloading clickhousectl ${chctl_tag}${RESET}"
            printf '%s\n' "    ${DIM}${chctl_url} -> ${chctl_install_dir}/clickhousectl${RESET}"
            chctl_tmp=$(mktemp -d)
            if mkdir -p "${chctl_install_dir}" \
                && curl -fsSL "${chctl_url}" -o "${chctl_tmp}/${chctl_archive}" \
                && tar -xzf "${chctl_tmp}/${chctl_archive}" -C "${chctl_tmp}" \
                && mv -f "${chctl_tmp}/clickhousectl-${chctl_target}-${chctl_tag}/clickhousectl" "${chctl_install_dir}/clickhousectl"
            then
                chmod a+x "${chctl_install_dir}/clickhousectl"
                ln -sf "${chctl_install_dir}/clickhousectl" "${chctl_install_dir}/chctl"
                chctl_installed=1
                printf '%s\n' "    installed to ${chctl_install_dir}/clickhousectl ${DIM}(alias: chctl)${RESET}"
                case ":$PATH:" in
                    *":${chctl_install_dir}:"*) ;;
                    *)
                        printf '%s\n' "    ${DIM}NOTE: ${chctl_install_dir} is not in your PATH. Add it with:${RESET}"
                        printf '%s\n' "      export PATH=\"${chctl_install_dir}:\$PATH\""
                        printf '%s\n' "    ${DIM}(you may want to add that line to your shell profile, e.g. ~/.bashrc or ~/.zshrc)${RESET}"
                        ;;
                esac
            else
                echo "Warning: failed to download clickhousectl. Continuing." >&2
            fi
            rm -rf "${chctl_tmp}"
        else
            echo "Warning: could not determine the latest clickhousectl release. Continuing." >&2
        fi
    fi
fi

# Pad commands so the descriptions line up, even when the binary got a
# non-clashing name like clickhouse.0. 27 = len("sudo ./clickhouse install") + 2.
PAD=$((17 + ${#clickhouse}))
[ "${PAD}" -lt 27 ] && PAD=27

echo
printf '%s\n' "${BOLD}Get started:${RESET}"
printf "  ${YELLOW}%-${PAD}s${RESET}%s\n" "./${clickhouse}" "Open an interactive ClickHouse shell using clickhouse-local"
printf "  ${YELLOW}%-${PAD}s${RESET}%s\n" "./${clickhouse} server" "Start a server in the current directory"
printf "  ${YELLOW}%-${PAD}s${RESET}%s\n" "sudo ./${clickhouse} install" "Install system-wide with config and a service"

if [ -n "${chctl_installed}" ]
then
    echo
    printf '%s\n' "${BOLD}For AI agents:${RESET}"
    printf "  ${YELLOW}%-${PAD}s${RESET}%s\n" "chctl skills" "Install the official ClickHouse Agent Skills"
    printf "  ${YELLOW}%-${PAD}s${RESET}%s\n" "chctl local" "Work with local ClickHouse"
    printf "  ${YELLOW}%-${PAD}s${RESET}%s\n" "chctl local postgres" "Work with local Postgres"
    printf "  ${YELLOW}%-${PAD}s${RESET}%s\n" "chctl cloud" "Work with ClickHouse Cloud, the managed service for ClickHouse and Postgres"
fi

echo
printf '%s\n' "${BOLD}Learn more:${RESET}"
printf '%s\n' "  Quick start       ${CYAN}https://clickhouse.com/docs/getting-started/quick-start${RESET}"
printf '%s\n' "  clickhousectl     ${CYAN}https://clickhouse.com/docs/interfaces/cli${RESET}"
printf '%s\n' "  ClickHouse Cloud  ${CYAN}https://clickhouse.com/cloud${RESET}"
printf '%s\n' "  Docs              ${CYAN}https://clickhouse.com/docs${RESET}"
printf '%s\n' "  Community         ${CYAN}https://clickhouse.com/slack${RESET}"
echo
