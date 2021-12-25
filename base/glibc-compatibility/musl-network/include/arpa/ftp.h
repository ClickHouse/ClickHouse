#ifndef _ARPA_FTP_H
#define _ARPA_FTP_H
#define PRELIM 1
#define COMPLETE 2
#define CONTINUE 3
#define TRANSIENT 4
#define ERROR 5
#define TYPE_A 1
#define TYPE_E 2
#define TYPE_I 3
#define TYPE_L 4
#define FORM_N 1
#define FORM_T 2
#define FORM_C 3
#define STRU_F 1
#define STRU_R 2
#define STRU_P 3
#define MODE_S 1
#define MODE_B 2
#define MODE_C 3
#define REC_ESC '\377'
#define REC_EOR '\001'
#define REC_EOF '\002'
#define BLK_EOR 0x80
#define BLK_EOF 0x40
#define BLK_ERRORS 0x20
#define BLK_RESTART 0x10
#define BLK_BYTECOUNT 2
#ifdef FTP_NAMES
char *modenames[] =  {"0", "Stream", "Block", "Compressed" };
char *strunames[] =  {"0", "File", "Record", "Page" };
char *typenames[] =  {"0", "ASCII", "EBCDIC", "Image", "Local" };
char *formnames[] =  {"0", "Nonprint", "Telnet", "Carriage-control" };
#endif
#endif
