#ifndef _SCSI_SG_H
#define _SCSI_SG_H

#define SG_DXFER_NONE -1
#define SG_DXFER_TO_DEV -2
#define SG_DXFER_FROM_DEV -3
#define SG_DXFER_TO_FROM_DEV -4
#define SG_FLAG_DIRECT_IO 1
#define SG_FLAG_LUN_INHIBIT 2
#define SG_FLAG_NO_DXFER 0x10000
#define SG_INFO_OK_MASK 0x1
#define SG_INFO_OK 0x0
#define SG_INFO_CHECK 0x1
#define SG_INFO_DIRECT_IO_MASK 0x6
#define SG_INFO_INDIRECT_IO 0x0
#define SG_INFO_DIRECT_IO 0x2
#define SG_INFO_MIXED_IO 0x4
#define SG_EMULATED_HOST 0x2203
#define SG_SET_TRANSFORM 0x2204
#define SG_GET_TRANSFORM 0x2205
#define SG_SET_RESERVED_SIZE 0x2275
#define SG_GET_RESERVED_SIZE 0x2272
#define SG_GET_SCSI_ID 0x2276
#define SG_SET_FORCE_LOW_DMA 0x2279
#define SG_GET_LOW_DMA 0x227a
#define SG_SET_FORCE_PACK_ID 0x227b
#define SG_GET_PACK_ID 0x227c
#define SG_GET_NUM_WAITING 0x227d
#define SG_GET_SG_TABLESIZE 0x227F
#define SG_GET_VERSION_NUM 0x2282
#define SG_SCSI_RESET 0x2284
#define SG_SCSI_RESET_NOTHING 0
#define SG_SCSI_RESET_DEVICE 1
#define SG_SCSI_RESET_BUS 2
#define SG_SCSI_RESET_HOST 3
#define SG_IO 0x2285
#define SG_GET_REQUEST_TABLE 0x2286
#define SG_SET_KEEP_ORPHAN 0x2287
#define SG_GET_KEEP_ORPHAN 0x2288
#define SG_SCATTER_SZ (8 * 4096)
#define SG_DEFAULT_RETRIES 1
#define SG_DEF_FORCE_LOW_DMA 0
#define SG_DEF_FORCE_PACK_ID 0
#define SG_DEF_KEEP_ORPHAN 0
#define SG_DEF_RESERVED_SIZE SG_SCATTER_SZ
#define SG_MAX_QUEUE 16
#define SG_BIG_BUFF SG_DEF_RESERVED_SIZE
#define SG_MAX_SENSE 16
#define SG_SET_TIMEOUT 0x2201
#define SG_GET_TIMEOUT 0x2202
#define SG_GET_COMMAND_Q 0x2270
#define SG_SET_COMMAND_Q 0x2271
#define SG_SET_DEBUG 0x227e
#define SG_NEXT_CMD_LEN 0x2283
#define SG_DEFAULT_TIMEOUT (60*100) /* 60*HZ */
#define SG_DEF_COMMAND_Q 0
#define SG_DEF_UNDERRUN_FLAG 0

typedef struct sg_iovec {
	void *iov_base;
	unsigned long iov_len;
} sg_iovec_t;

typedef struct sg_io_hdr { 
	int interface_id; 
	int dxfer_direction; 
	unsigned char cmd_len;
	unsigned char mx_sb_len;
	unsigned short iovec_count;
	unsigned dxfer_len;
	void *dxferp;
	unsigned char *cmdp;
	unsigned char *sbp;
	unsigned timeout;
	unsigned flags;
	int pack_id;
	void *usr_ptr;
	unsigned char status;
	unsigned char masked_status;
	unsigned char msg_status;
	unsigned char sb_len_wr;
	unsigned short host_status;
	unsigned short driver_status;
	int resid; 
	unsigned int duration;
	unsigned int info;
} sg_io_hdr_t;

struct sg_scsi_id {
	int host_no;
	int channel;
	int scsi_id;
	int lun;
	int scsi_type;
	short h_cmd_per_lun;
	short d_queue_depth;
	int unused[2];
};

typedef struct sg_req_info {
	char req_state;
	char orphan;
	char sg_io_owned;
	char problem;
	int pack_id;
	void *usr_ptr;
	unsigned duration; 
	int unused; 
} sg_req_info_t;

typedef struct sg_io_hdr Sg_io_hdr;
typedef struct sg_io_vec Sg_io_vec;
typedef struct sg_scsi_id Sg_scsi_id;
typedef struct sg_req_info Sg_req_info;

struct sg_header {
	int pack_len;
	int reply_len;
	int pack_id;
	int result;
	unsigned twelve_byte:1;
	unsigned target_status:5;
	unsigned host_status:8;
	unsigned driver_status:8;
	unsigned other_flags:10;
	unsigned char sense_buffer[SG_MAX_SENSE];
};

#endif
