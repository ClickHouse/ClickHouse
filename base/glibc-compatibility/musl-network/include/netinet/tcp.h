#ifndef _NETINET_TCP_H
#define _NETINET_TCP_H

#include <features.h>

#define TCP_NODELAY 1
#define TCP_MAXSEG	 2
#define TCP_CORK	 3
#define TCP_KEEPIDLE	 4
#define TCP_KEEPINTVL	 5
#define TCP_KEEPCNT	 6
#define TCP_SYNCNT	 7
#define TCP_LINGER2	 8
#define TCP_DEFER_ACCEPT 9
#define TCP_WINDOW_CLAMP 10
#define TCP_INFO	 11
#define	TCP_QUICKACK	 12
#define TCP_CONGESTION	 13
#define TCP_MD5SIG	 14
#define TCP_THIN_LINEAR_TIMEOUTS 16
#define TCP_THIN_DUPACK  17
#define TCP_USER_TIMEOUT 18
#define TCP_REPAIR       19
#define TCP_REPAIR_QUEUE 20
#define TCP_QUEUE_SEQ    21
#define TCP_REPAIR_OPTIONS 22
#define TCP_FASTOPEN     23
#define TCP_TIMESTAMP    24
#define TCP_NOTSENT_LOWAT 25
#define TCP_CC_INFO      26
#define TCP_SAVE_SYN     27
#define TCP_SAVED_SYN    28
#define TCP_REPAIR_WINDOW 29
#define TCP_FASTOPEN_CONNECT 30
#define TCP_ULP          31
#define TCP_MD5SIG_EXT   32
#define TCP_FASTOPEN_KEY 33
#define TCP_FASTOPEN_NO_COOKIE 34
#define TCP_ZEROCOPY_RECEIVE   35
#define TCP_INQ          36
#define TCP_TX_DELAY     37

#define TCP_CM_INQ TCP_INQ

#define TCP_ESTABLISHED  1
#define TCP_SYN_SENT     2
#define TCP_SYN_RECV     3
#define TCP_FIN_WAIT1    4
#define TCP_FIN_WAIT2    5
#define TCP_TIME_WAIT    6
#define TCP_CLOSE        7
#define TCP_CLOSE_WAIT   8
#define TCP_LAST_ACK     9
#define TCP_LISTEN       10
#define TCP_CLOSING      11

enum {
	TCP_NLA_PAD,
	TCP_NLA_BUSY,
	TCP_NLA_RWND_LIMITED,
	TCP_NLA_SNDBUF_LIMITED,
	TCP_NLA_DATA_SEGS_OUT,
	TCP_NLA_TOTAL_RETRANS,
	TCP_NLA_PACING_RATE,
	TCP_NLA_DELIVERY_RATE,
	TCP_NLA_SND_CWND,
	TCP_NLA_REORDERING,
	TCP_NLA_MIN_RTT,
	TCP_NLA_RECUR_RETRANS,
	TCP_NLA_DELIVERY_RATE_APP_LMT,
	TCP_NLA_SNDQ_SIZE,
	TCP_NLA_CA_STATE,
	TCP_NLA_SND_SSTHRESH,
	TCP_NLA_DELIVERED,
	TCP_NLA_DELIVERED_CE,
	TCP_NLA_BYTES_SENT,
	TCP_NLA_BYTES_RETRANS,
	TCP_NLA_DSACK_DUPS,
	TCP_NLA_REORD_SEEN,
	TCP_NLA_SRTT,
	TCP_NLA_TIMEOUT_REHASH,
	TCP_NLA_BYTES_NOTSENT,
};

#if defined(_GNU_SOURCE) || defined(_BSD_SOURCE)
#define TCPOPT_EOL              0
#define TCPOPT_NOP              1
#define TCPOPT_MAXSEG           2
#define TCPOPT_WINDOW           3
#define TCPOPT_SACK_PERMITTED   4
#define TCPOPT_SACK             5
#define TCPOPT_TIMESTAMP        8
#define TCPOLEN_SACK_PERMITTED  2
#define TCPOLEN_WINDOW          3
#define TCPOLEN_MAXSEG          4
#define TCPOLEN_TIMESTAMP       10

#define SOL_TCP 6

#include <sys/types.h>
#include <sys/socket.h>
#include <stdint.h>

typedef uint32_t tcp_seq;

#define TH_FIN 0x01
#define TH_SYN 0x02
#define TH_RST 0x04
#define TH_PUSH 0x08
#define TH_ACK 0x10
#define TH_URG 0x20

struct tcphdr {
#ifdef _GNU_SOURCE
#ifdef __GNUC__
	__extension__
#endif
	union { struct {

	uint16_t source;
	uint16_t dest;
	uint32_t seq;
	uint32_t ack_seq;
#if __BYTE_ORDER == __LITTLE_ENDIAN
	uint16_t res1:4;
	uint16_t doff:4;
	uint16_t fin:1;
	uint16_t syn:1;
	uint16_t rst:1;
	uint16_t psh:1;
	uint16_t ack:1;
	uint16_t urg:1;
	uint16_t res2:2;
#else
	uint16_t doff:4;
	uint16_t res1:4;
	uint16_t res2:2;
	uint16_t urg:1;
	uint16_t ack:1;
	uint16_t psh:1;
	uint16_t rst:1;
	uint16_t syn:1;
	uint16_t fin:1;
#endif
	uint16_t window;
	uint16_t check;
	uint16_t urg_ptr;

	}; struct {
#endif

	uint16_t th_sport;
	uint16_t th_dport;
	uint32_t th_seq;
	uint32_t th_ack;
#if __BYTE_ORDER == __LITTLE_ENDIAN
	uint8_t th_x2:4;
	uint8_t th_off:4;
#else
	uint8_t th_off:4;
	uint8_t th_x2:4;
#endif
	uint8_t th_flags;
	uint16_t th_win;
	uint16_t th_sum;
	uint16_t th_urp;

#ifdef _GNU_SOURCE
	}; };
#endif
};
#endif

#ifdef _GNU_SOURCE
#define TCPI_OPT_TIMESTAMPS	1
#define TCPI_OPT_SACK		2
#define TCPI_OPT_WSCALE		4
#define TCPI_OPT_ECN		8

#define TCP_CA_Open		0
#define TCP_CA_Disorder		1
#define TCP_CA_CWR		2
#define TCP_CA_Recovery		3
#define TCP_CA_Loss		4

enum tcp_fastopen_client_fail {
	TFO_STATUS_UNSPEC,
	TFO_COOKIE_UNAVAILABLE,
	TFO_DATA_NOT_ACKED,
	TFO_SYN_RETRANSMITTED,
};

struct tcp_info {
	uint8_t tcpi_state;
	uint8_t tcpi_ca_state;
	uint8_t tcpi_retransmits;
	uint8_t tcpi_probes;
	uint8_t tcpi_backoff;
	uint8_t tcpi_options;
	uint8_t tcpi_snd_wscale : 4, tcpi_rcv_wscale : 4;
	uint8_t tcpi_delivery_rate_app_limited : 1, tcpi_fastopen_client_fail : 2;
	uint32_t tcpi_rto;
	uint32_t tcpi_ato;
	uint32_t tcpi_snd_mss;
	uint32_t tcpi_rcv_mss;
	uint32_t tcpi_unacked;
	uint32_t tcpi_sacked;
	uint32_t tcpi_lost;
	uint32_t tcpi_retrans;
	uint32_t tcpi_fackets;
	uint32_t tcpi_last_data_sent;
	uint32_t tcpi_last_ack_sent;
	uint32_t tcpi_last_data_recv;
	uint32_t tcpi_last_ack_recv;
	uint32_t tcpi_pmtu;
	uint32_t tcpi_rcv_ssthresh;
	uint32_t tcpi_rtt;
	uint32_t tcpi_rttvar;
	uint32_t tcpi_snd_ssthresh;
	uint32_t tcpi_snd_cwnd;
	uint32_t tcpi_advmss;
	uint32_t tcpi_reordering;
	uint32_t tcpi_rcv_rtt;
	uint32_t tcpi_rcv_space;
	uint32_t tcpi_total_retrans;
	uint64_t tcpi_pacing_rate;
	uint64_t tcpi_max_pacing_rate;
	uint64_t tcpi_bytes_acked;
	uint64_t tcpi_bytes_received;
	uint32_t tcpi_segs_out;
	uint32_t tcpi_segs_in;
	uint32_t tcpi_notsent_bytes;
	uint32_t tcpi_min_rtt;
	uint32_t tcpi_data_segs_in;
	uint32_t tcpi_data_segs_out;
	uint64_t tcpi_delivery_rate;
	uint64_t tcpi_busy_time;
	uint64_t tcpi_rwnd_limited;
	uint64_t tcpi_sndbuf_limited;
	uint32_t tcpi_delivered;
	uint32_t tcpi_delivered_ce;
	uint64_t tcpi_bytes_sent;
	uint64_t tcpi_bytes_retrans;
	uint32_t tcpi_dsack_dups;
	uint32_t tcpi_reord_seen;
	uint32_t tcpi_rcv_ooopack;
	uint32_t tcpi_snd_wnd;
};

#define TCP_MD5SIG_MAXKEYLEN    80

#define TCP_MD5SIG_FLAG_PREFIX  0x1
#define TCP_MD5SIG_FLAG_IFINDEX 0x2

struct tcp_md5sig {
	struct sockaddr_storage tcpm_addr;
	uint8_t tcpm_flags;
	uint8_t tcpm_prefixlen;
	uint16_t tcpm_keylen;
	int tcpm_ifindex;
	uint8_t tcpm_key[TCP_MD5SIG_MAXKEYLEN];
};

struct tcp_diag_md5sig {
	uint8_t tcpm_family;
	uint8_t tcpm_prefixlen;
	uint16_t tcpm_keylen;
	uint32_t tcpm_addr[4];
	uint8_t tcpm_key[TCP_MD5SIG_MAXKEYLEN];
};

#define TCP_REPAIR_ON		1
#define TCP_REPAIR_OFF		0
#define TCP_REPAIR_OFF_NO_WP	-1

struct tcp_repair_window {
	uint32_t snd_wl1;
	uint32_t snd_wnd;
	uint32_t max_window;
	uint32_t rcv_wnd;
	uint32_t rcv_wup;
};

struct tcp_zerocopy_receive {
	uint64_t address;
	uint32_t length;
	uint32_t recv_skip_hint;
	uint32_t inq;
	int32_t err;
};

#endif

#endif
