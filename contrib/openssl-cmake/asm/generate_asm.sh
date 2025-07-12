#!/usr/bin/env bash
set -euo pipefail

OPENSSL_SOURCE_DIR="${1:-${OPENSSL_SOURCE_DIR:-$(pwd)/../../openssl}}"
OPENSSL_BINARY_DIR="${2:-${OPENSSL_BINARY_DIR:-$(pwd)}}"
CC="${CC:-cc}"

mkdir_and_run() {
    local file_in="$1"
    local file_out="$2"
    shift 2
    local args=("$@")
    echo "Generating ${file_out}"
    mkdir -p "$(dirname "$file_out")"
    CC="$CC" perl "$file_in" "${args[@]}" "$file_out"
}

declare -a jobs=(
    # ARCH_AMD64
    "crypto/aes/asm/aes-x86_64.pl crypto/aes/aes-x86_64.s"
    "crypto/aes/asm/aesni-mb-x86_64.pl crypto/aes/aesni-mb-x86_64.s"
    "crypto/aes/asm/aesni-sha1-x86_64.pl crypto/aes/aesni-sha1-x86_64.s"
    "crypto/aes/asm/aesni-sha256-x86_64.pl crypto/aes/aesni-sha256-x86_64.s"
    "crypto/aes/asm/aesni-x86_64.pl crypto/aes/aesni-x86_64.s"
    "crypto/aes/asm/aesni-xts-avx512.pl crypto/aes/aesni-xts-avx512.s"
    "crypto/aes/asm/bsaes-x86_64.pl crypto/aes/bsaes-x86_64.s"
    "crypto/aes/asm/vpaes-x86_64.pl crypto/aes/vpaes-x86_64.s"
    "crypto/bn/asm/rsaz-2k-avx512.pl crypto/bn/rsaz-2k-avx512.s"
    "crypto/bn/asm/rsaz-3k-avx512.pl crypto/bn/rsaz-3k-avx512.s"
    "crypto/bn/asm/rsaz-4k-avx512.pl crypto/bn/rsaz-4k-avx512.s"
    "crypto/bn/asm/rsaz-2k-avxifma.pl crypto/bn/rsaz-2k-avxifma.s"
    "crypto/bn/asm/rsaz-3k-avxifma.pl crypto/bn/rsaz-3k-avxifma.s"
    "crypto/bn/asm/rsaz-4k-avxifma.pl crypto/bn/rsaz-4k-avxifma.s"
    "crypto/bn/asm/rsaz-avx2.pl crypto/bn/rsaz-avx2.s"
    "crypto/bn/asm/rsaz-x86_64.pl crypto/bn/rsaz-x86_64.s"
    "crypto/bn/asm/x86_64-gf2m.pl crypto/bn/x86_64-gf2m.s"
    "crypto/bn/asm/x86_64-mont.pl crypto/bn/x86_64-mont.s"
    "crypto/bn/asm/x86_64-mont5.pl crypto/bn/x86_64-mont5.s"
    "crypto/camellia/asm/cmll-x86_64.pl crypto/camellia/cmll-x86_64.s"
    "crypto/chacha/asm/chacha-x86_64.pl crypto/chacha/chacha-x86_64.s"
    "crypto/ec/asm/ecp_nistz256-x86_64.pl crypto/ec/ecp_nistz256-x86_64.s"
    "crypto/ec/asm/x25519-x86_64.pl crypto/ec/x25519-x86_64.s"
    "crypto/md5/asm/md5-x86_64.pl crypto/md5/md5-x86_64.s"
    "crypto/modes/asm/aesni-gcm-x86_64.pl crypto/modes/aesni-gcm-x86_64.s"
    "crypto/modes/asm/aes-gcm-avx512.pl crypto/modes/aes-gcm-avx512.s"
    "crypto/x86_64cpuid.pl crypto/x86_64cpuid.s"
    "crypto/modes/asm/ghash-x86_64.pl crypto/modes/ghash-x86_64.s"
    "crypto/poly1305/asm/poly1305-x86_64.pl crypto/poly1305/poly1305-x86_64.s"
    "crypto/rc4/asm/rc4-md5-x86_64.pl crypto/rc4/rc4-md5-x86_64.s"
    "crypto/rc4/asm/rc4-x86_64.pl crypto/rc4/rc4-x86_64.s"
    "crypto/sha/asm/keccak1600-x86_64.pl crypto/sha/keccak1600-x86_64.s"
    "crypto/sha/asm/sha1-mb-x86_64.pl crypto/sha/sha1-mb-x86_64.s"
    "crypto/sha/asm/sha1-x86_64.pl crypto/sha/sha1-x86_64.s"
    "crypto/sha/asm/sha256-mb-x86_64.pl crypto/sha/sha256-mb-x86_64.s"
    "crypto/sha/asm/sha512-x86_64.pl crypto/sha/sha256-x86_64.s"
    "crypto/sha/asm/sha512-x86_64.pl crypto/sha/sha512-x86_64.s"
    "crypto/whrlpool/asm/wp-x86_64.pl crypto/whrlpool/wp-x86_64.s"

    # ARCH_AARCH64
    "crypto/aes/asm/aesv8-armx.pl crypto/aes/aesv8-armx.S linux64"
    "crypto/aes/asm/bsaes-armv8.pl crypto/aes/bsaes-armv8.S linux64"
    "crypto/aes/asm/vpaes-armv8.pl crypto/aes/vpaes-armv8.S linux64"
    "crypto/bn/asm/armv8-mont.pl crypto/bn/armv8-mont.S linux64"
    "crypto/chacha/asm/chacha-armv8.pl crypto/chacha/chacha-armv8.S linux64"
    "crypto/chacha/asm/chacha-armv8-sve.pl crypto/chacha/chacha-armv8-sve.S linux64"
    "crypto/ec/asm/ecp_nistz256-armv8.pl crypto/ec/ecp_nistz256-armv8.S linux64"
    "crypto/ec/asm/ecp_sm2p256-armv8.pl crypto/ec/ecp_sm2p256-armv8.S linux64"
    "crypto/arm64cpuid.pl crypto/arm64cpuid.S linux64"
    "crypto/modes/asm/ghashv8-armx.pl crypto/modes/ghashv8-armx.S linux64"
    "crypto/poly1305/asm/poly1305-armv8.pl crypto/poly1305/poly1305-armv8.S linux64"
    "crypto/sha/asm/keccak1600-armv8.pl crypto/sha/keccak1600-armv8.S linux64"
    "crypto/sha/asm/sha1-armv8.pl crypto/sha/sha1-armv8.S linux64"
    "crypto/sha/asm/sha512-armv8.pl crypto/sha/sha256-armv8.S linux64"
    "crypto/sha/asm/sha512-armv8.pl crypto/sha/sha512-armv8.S linux64"
    "crypto/modes/asm/aes-gcm-armv8_64.pl crypto/modes/asm/aes-gcm-armv8_64.S linux64"
    "crypto/modes/asm/aes-gcm-armv8-unroll8_64.pl crypto/modes/asm/aes-gcm-armv8-unroll8_64.S linux64"
    "crypto/sm3/asm/sm3-armv8.pl crypto/sm3/asm/sm3-armv8.S linux64"
    "crypto/sm4/asm/sm4-armv8.pl crypto/sm4/asm/sm4-armv8.S linux64"
    "crypto/sm4/asm/vpsm4-armv8.pl crypto/sm4/asm/vpsm4-armv8.S linux64"
    "crypto/sm4/asm/vpsm4_ex-armv8.pl crypto/sm4/asm/vpsm4_ex-armv8.S linux64"
    "crypto/md5/asm/md5-aarch64.pl crypto/md5/asm/md5-aarch64.S linux64"

    # ARCH_PPC64LE
    "crypto/aes/asm/aesp8-ppc.pl crypto/aes/aesp8-ppc.s linux64v2"
    "crypto/modes/asm/ghashp8-ppc.pl crypto/modes/ghashp8-ppc.s linux64v2"
    "crypto/ppccpuid.pl crypto/ppccpuid.s linux64v2"
    "crypto/modes/asm/aes-gcm-ppc.pl crypto/modes/aes-gcm-ppc.s linux64v2"

    # ARCH_S390X
    "crypto/aes/asm/aes-s390x.pl crypto/aes/aes-s390x.S linux64"
    "crypto/s390xcpuid.pl crypto/s390xcpuid.S linux64"
    "crypto/chacha/asm/chacha-s390x.pl crypto/chacha/chacha-s390x.S linux64"
    "crypto/rc4/asm/rc4-s390x.pl crypto/rc4/rc4-s390x.S linux64"
    "crypto/sha/asm/keccak1600-s390x.pl crypto/sha/keccak1600-s390x.S linux64"

    # ARCH_RISCV64
    "crypto/riscv64cpuid.pl crypto/riscv64cpuid.S linux64"
    "crypto/aes/asm/aes-riscv64-zkn.pl crypto/aes/aes-riscv64-zkn.S linux64"
    "crypto/modes/asm/ghash-riscv64.pl crypto/modes/ghash-riscv64.S linux64"

    # ARCH_LOONGARCH64
    "crypto/loongarch64cpuid.pl crypto/loongarch64cpuid.S linux64"
)

for entry in "${jobs[@]}"; do
    read -r rel_in rel_out arg <<<"${entry:-}"
    full_in="$OPENSSL_SOURCE_DIR/$rel_in"
    full_out="$OPENSSL_BINARY_DIR/$rel_out"

    if [[ -n "${arg:-}" ]]; then
        mkdir_and_run "$full_in" "$full_out" "$arg"
    else
        mkdir_and_run "$full_in" "$full_out"
    fi
done
