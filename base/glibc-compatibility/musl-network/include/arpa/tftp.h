#ifndef _ARPA_TFTP_H
#define _ARPA_TFTP_H
#define SEGSIZE 512
#define RRQ 01
#define WRQ 02
#define DATA 03
#define ACK 04
#define ERROR 05
struct tftphdr {
	short th_opcode;
	union {
		unsigned short tu_block;
		short tu_code;
		char tu_stuff[1];
	} th_u;
	char th_data[1];
};
#define th_block th_u.tu_block
#define th_code th_u.tu_code
#define th_stuff th_u.tu_stuff
#define th_msg th_data
#define EUNDEF 0
#define ENOTFOUND 1
#define EACCESS 2
#define ENOSPACE 3
#define EBADOP 4
#define EBADID 5
#define EEXISTS 6
#define ENOUSER 7
#endif

