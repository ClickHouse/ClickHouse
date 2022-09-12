; This file is generated from a similarly-named Perl script in the BoringSSL
; source tree. Do not edit by hand.

default	rel
%define XMMWORD
%define YMMWORD
%define ZMMWORD

%ifdef BORINGSSL_PREFIX
%include "boringssl_prefix_symbols_nasm.inc"
%endif
global	dummy_chacha20_poly1305_asm

dummy_chacha20_poly1305_asm:
	DB	0F3h,0C3h		;repret
