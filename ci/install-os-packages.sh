#!/usr/bin/env bash
set -e -x

# Dispatches package installation on various OS and distributives

WHAT=$1

[[ $EUID -ne 0 ]] && SUDO=sudo

command -v apt-get && PACKAGE_MANAGER=apt
command -v yum && PACKAGE_MANAGER=yum
command -v pkg && PACKAGE_MANAGER=pkg


case $PACKAGE_MANAGER in
    apt)
        case $WHAT in
            prepare)
                $SUDO apt-get update
                ;;
            svn)
                $SUDO apt-get install -y subversion
                ;;
            gcc*)
                $SUDO apt-get install -y $WHAT ${WHAT/cc/++}
                ;;
            clang*)
                $SUDO apt-get install -y $WHAT libc++-dev libc++abi-dev
                [[ $(uname -m) == "x86_64" ]] && $SUDO apt-get install -y ${WHAT/clang/lld} || true
                ;;
            git)
                $SUDO apt-get install -y git
                ;;
            cmake)
                $SUDO apt-get install -y cmake3 || $SUDO apt-get install -y cmake
                ;;
            curl)
                $SUDO apt-get install -y curl
                ;;
            jq)
                $SUDO apt-get install -y jq
                ;;
            libssl-dev)
                $SUDO apt-get install -y libssl-dev
                ;;
            libicu-dev)
                $SUDO apt-get install -y libicu-dev
                ;;
            libreadline-dev)
                $SUDO apt-get install -y libreadline-dev
                ;;
            libunixodbc-dev)
                $SUDO apt-get install -y unixodbc-dev
                ;;
            libmariadbclient-dev)
                $SUDO apt-get install -y libmariadbclient-dev
                ;;
            llvm-libs*)
                $SUDO apt-get install -y ${WHAT/llvm-libs/liblld}-dev ${WHAT/llvm-libs/libclang}-dev
                ;;
            qemu-user-static)
                $SUDO apt-get install -y qemu-user-static
                ;;
            vagrant-virtualbox)
                $SUDO apt-get install -y vagrant virtualbox
                ;;
            *)
                echo "Unknown package"; exit 1;
                ;;
        esac
        ;;
    pkg)
        case $WHAT in
            prepare)
                ;;
            svn)
                $SUDO pkg install -y subversion
                ;;
            gcc*)
                $SUDO pkg install -y ${WHAT/-/}
                ;;
            clang*)
                $SUDO pkg install -y clang-devel
                ;;
            git)
                $SUDO pkg install -y git
                ;;
            cmake)
                $SUDO pkg install -y cmake
                ;;
            curl)
                $SUDO pkg install -y curl
                ;;
            jq)
                $SUDO pkg install -y jq
                ;;
            libssl-dev)
                $SUDO pkg install -y openssl
                ;;
            libicu-dev)
                $SUDO pkg install -y icu
                ;;
            libreadline-dev)
                $SUDO pkg install -y readline
                ;;
            libunixodbc-dev)
                $SUDO pkg install -y unixODBC libltdl
                ;;
            libmariadbclient-dev)
                $SUDO pkg install -y mariadb102-client
                ;;
            *)
                echo "Unknown package"; exit 1;
                ;;
        esac
        ;;
    *)
        echo "Unknown distributive"; exit 1;
        ;;
esac
