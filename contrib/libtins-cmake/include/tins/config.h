/* Hand-written libtins configuration for the ClickHouse build.
 *
 * Mirrors include/tins/config.h.in. Features are fixed here rather than probed:
 *  - C++11 is always available in the ClickHouse toolchain.
 *  - IEEE 802.11 (DOT11), TCP/IP reassembly and ACK tracking are enabled.
 *  - libpcap is present (we build it under contrib/libpcap-cmake).
 *  - WPA2 decryption is DISABLED to avoid pulling OpenSSL into libtins for now.
 *    (crypto.cpp / handshake_capturer.cpp still compile; they simply omit the
 *    WPA2 code paths.)
 */

#ifndef TINS_CONFIG_H
#define TINS_CONFIG_H

#define TINS_HAVE_CXX11

#define TINS_HAVE_DOT11

/* WPA2 decryption intentionally left disabled (would require OpenSSL). */
/* #undef TINS_HAVE_WPA2_DECRYPTION */
/* #undef TINS_HAVE_WPA2_CALLBACKS */

/* We only ever read savefiles; no live sending. */
/* #undef TINS_HAVE_PACKET_SENDER_PCAP_SENDPACKET */

#define TINS_HAVE_TCPIP

/* ACK tracking pulls in Boost.ICL, which is not part of ClickHouse's bundled
 * Boost subset and is not needed for L2-L4 dissection. Leave it disabled. */
/* #undef TINS_HAVE_ACK_TRACKER */

/* #undef TINS_HAVE_TCP_STREAM_CUSTOM_DATA */

#define TINS_HAVE_GCC_BUILTIN_SWAP

#define TINS_HAVE_PCAP

#define TINS_VERSION_MAJOR 4
#define TINS_VERSION_MINOR 5
#define TINS_VERSION_PATCH 0

#endif // TINS_CONFIG_H
