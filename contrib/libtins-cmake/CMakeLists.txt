# libtins - packet crafting and dissection library, used by the PCAP input format.
#
# We build it from source with a hand-written include/tins/config.h (see this
# directory). WPA2 decryption is disabled to avoid an OpenSSL dependency for now;
# everything else (Ethernet/IP/TCP/UDP dissection, 802.11, TCP reassembly) is on.
#
# libtins reads capture files through libpcap, which we build in
# contrib/libpcap-cmake (added before this directory in contrib/CMakeLists.txt).

# ENABLE_PCAP is declared in contrib/CMakeLists.txt (before libpcap-cmake).

if (NOT ENABLE_PCAP OR NOT OS_LINUX)
    message(STATUS "Not using libtins (the PCAP input format is supported only on Linux)")
    return()
endif()

if (NOT TARGET ch_contrib::pcap)
    message(STATUS "libpcap target is not available, disabling PCAP input format")
    return()
endif()

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/libtins")

set(SRCS
    "${LIBRARY_DIR}/src/address_range.cpp"
    "${LIBRARY_DIR}/src/arp.cpp"
    "${LIBRARY_DIR}/src/bootp.cpp"
    "${LIBRARY_DIR}/src/crypto.cpp"
    "${LIBRARY_DIR}/src/dhcp.cpp"
    "${LIBRARY_DIR}/src/dhcpv6.cpp"
    "${LIBRARY_DIR}/src/dns.cpp"
    "${LIBRARY_DIR}/src/dot1q.cpp"
    "${LIBRARY_DIR}/src/dot3.cpp"
    "${LIBRARY_DIR}/src/eapol.cpp"
    "${LIBRARY_DIR}/src/ethernetII.cpp"
    "${LIBRARY_DIR}/src/handshake_capturer.cpp"
    "${LIBRARY_DIR}/src/hw_address.cpp"
    "${LIBRARY_DIR}/src/icmp.cpp"
    "${LIBRARY_DIR}/src/icmp_extension.cpp"
    "${LIBRARY_DIR}/src/icmpv6.cpp"
    "${LIBRARY_DIR}/src/ip.cpp"
    "${LIBRARY_DIR}/src/ip_address.cpp"
    "${LIBRARY_DIR}/src/ip_reassembler.cpp"
    "${LIBRARY_DIR}/src/ipsec.cpp"
    "${LIBRARY_DIR}/src/ipv6.cpp"
    "${LIBRARY_DIR}/src/ipv6_address.cpp"
    "${LIBRARY_DIR}/src/llc.cpp"
    "${LIBRARY_DIR}/src/loopback.cpp"
    "${LIBRARY_DIR}/src/memory_helpers.cpp"
    "${LIBRARY_DIR}/src/mpls.cpp"
    "${LIBRARY_DIR}/src/network_interface.cpp"
    "${LIBRARY_DIR}/src/offline_packet_filter.cpp"
    "${LIBRARY_DIR}/src/packet_sender.cpp"
    "${LIBRARY_DIR}/src/packet_writer.cpp"
    "${LIBRARY_DIR}/src/pdu.cpp"
    "${LIBRARY_DIR}/src/pdu_iterator.cpp"
    "${LIBRARY_DIR}/src/pdu_option.cpp"
    "${LIBRARY_DIR}/src/pktap.cpp"
    "${LIBRARY_DIR}/src/ppi.cpp"
    "${LIBRARY_DIR}/src/pppoe.cpp"
    "${LIBRARY_DIR}/src/radiotap.cpp"
    "${LIBRARY_DIR}/src/rawpdu.cpp"
    "${LIBRARY_DIR}/src/rsn_information.cpp"
    "${LIBRARY_DIR}/src/sll.cpp"
    "${LIBRARY_DIR}/src/snap.cpp"
    "${LIBRARY_DIR}/src/sniffer.cpp"
    "${LIBRARY_DIR}/src/stp.cpp"
    "${LIBRARY_DIR}/src/tcp.cpp"
    "${LIBRARY_DIR}/src/tcp_stream.cpp"
    "${LIBRARY_DIR}/src/timestamp.cpp"
    "${LIBRARY_DIR}/src/udp.cpp"
    "${LIBRARY_DIR}/src/vxlan.cpp"
    "${LIBRARY_DIR}/src/dot11/dot11_assoc.cpp"
    "${LIBRARY_DIR}/src/dot11/dot11_auth.cpp"
    "${LIBRARY_DIR}/src/dot11/dot11_base.cpp"
    "${LIBRARY_DIR}/src/dot11/dot11_beacon.cpp"
    "${LIBRARY_DIR}/src/dot11/dot11_control.cpp"
    "${LIBRARY_DIR}/src/dot11/dot11_data.cpp"
    "${LIBRARY_DIR}/src/dot11/dot11_mgmt.cpp"
    "${LIBRARY_DIR}/src/dot11/dot11_probe.cpp"
    "${LIBRARY_DIR}/src/tcp_ip/ack_tracker.cpp"
    "${LIBRARY_DIR}/src/tcp_ip/data_tracker.cpp"
    "${LIBRARY_DIR}/src/tcp_ip/flow.cpp"
    "${LIBRARY_DIR}/src/tcp_ip/stream.cpp"
    "${LIBRARY_DIR}/src/tcp_ip/stream_follower.cpp"
    "${LIBRARY_DIR}/src/tcp_ip/stream_identifier.cpp"
    "${LIBRARY_DIR}/src/utils/checksum_utils.cpp"
    "${LIBRARY_DIR}/src/utils/frequency_utils.cpp"
    "${LIBRARY_DIR}/src/utils/pdu_utils.cpp"
    "${LIBRARY_DIR}/src/utils/radiotap_parser.cpp"
    "${LIBRARY_DIR}/src/utils/radiotap_writer.cpp"
    "${LIBRARY_DIR}/src/utils/resolve_utils.cpp"
    "${LIBRARY_DIR}/src/utils/routing_utils.cpp"
    "${LIBRARY_DIR}/src/detail/address_helpers.cpp"
    "${LIBRARY_DIR}/src/detail/icmp_extension_helpers.cpp"
    "${LIBRARY_DIR}/src/detail/pdu_helpers.cpp"
    "${LIBRARY_DIR}/src/detail/sequence_number_helpers.cpp"
)

add_library(_tins ${SRCS})
add_library(ch_contrib::tins ALIAS _tins)

# Our hand-written include/tins/config.h must be found BEFORE the submodule's
# include directory (which only has config.h.in, not config.h).
target_include_directories(_tins SYSTEM BEFORE PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${LIBRARY_DIR}/include")

# tcp_ip/* uses Boost headers (e.g. functional/hash); pull in the header-only set.
target_link_libraries(_tins PUBLIC ch_contrib::pcap PRIVATE boost::headers_only)
