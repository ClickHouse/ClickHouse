#ifndef _ARPA_TELNET_H
#define	_ARPA_TELNET_H

#define	IAC	255
#define	DONT	254
#define	DO	253
#define	WONT	252
#define	WILL	251
#define	SB	250
#define	GA	249
#define	EL	248
#define	EC	247
#define	AYT	246
#define	AO	245
#define	IP	244
#define	BREAK	243
#define	DM	242
#define	NOP	241
#define	SE	240
#define EOR     239
#define	ABORT	238
#define	SUSP	237
#define	xEOF	236

#define SYNCH	242

#define telcmds ((char [][6]){ "EOF", "SUSP", "ABORT", "EOR", "SE", "NOP", "DMARK", "BRK", "IP", "AO", "AYT", "EC", "EL", "GA", "SB", "WILL", "WONT", "DO", "DONT", "IAC", 0 })

#define	TELCMD_FIRST	xEOF
#define	TELCMD_LAST	IAC
#define	TELCMD_OK(x)	((unsigned int)(x) <= TELCMD_LAST && \
			 (unsigned int)(x) >= TELCMD_FIRST)
#define	TELCMD(x)	telcmds[(x)-TELCMD_FIRST]

#define TELOPT_BINARY	0
#define TELOPT_ECHO	1
#define	TELOPT_RCP	2
#define	TELOPT_SGA	3
#define	TELOPT_NAMS	4
#define	TELOPT_STATUS	5
#define	TELOPT_TM	6
#define	TELOPT_RCTE	7
#define TELOPT_NAOL 	8
#define TELOPT_NAOP 	9
#define TELOPT_NAOCRD	10
#define TELOPT_NAOHTS	11
#define TELOPT_NAOHTD	12
#define TELOPT_NAOFFD	13
#define TELOPT_NAOVTS	14
#define TELOPT_NAOVTD	15
#define TELOPT_NAOLFD	16
#define TELOPT_XASCII	17
#define	TELOPT_LOGOUT	18
#define	TELOPT_BM	19
#define	TELOPT_DET	20
#define	TELOPT_SUPDUP	21
#define	TELOPT_SUPDUPOUTPUT 22
#define	TELOPT_SNDLOC	23
#define	TELOPT_TTYPE	24
#define	TELOPT_EOR	25
#define	TELOPT_TUID	26
#define	TELOPT_OUTMRK	27
#define	TELOPT_TTYLOC	28
#define	TELOPT_3270REGIME 29
#define	TELOPT_X3PAD	30
#define	TELOPT_NAWS	31
#define	TELOPT_TSPEED	32
#define	TELOPT_LFLOW	33
#define TELOPT_LINEMODE	34
#define TELOPT_XDISPLOC	35
#define TELOPT_OLD_ENVIRON 36
#define	TELOPT_AUTHENTICATION 37/* Authenticate */
#define	TELOPT_ENCRYPT	38
#define TELOPT_NEW_ENVIRON 39
#define	TELOPT_EXOPL	255


#define	NTELOPTS	(1+TELOPT_NEW_ENVIRON)
#ifdef TELOPTS
char *telopts[NTELOPTS+1] = {
	"BINARY", "ECHO", "RCP", "SUPPRESS GO AHEAD", "NAME",
	"STATUS", "TIMING MARK", "RCTE", "NAOL", "NAOP",
	"NAOCRD", "NAOHTS", "NAOHTD", "NAOFFD", "NAOVTS",
	"NAOVTD", "NAOLFD", "EXTEND ASCII", "LOGOUT", "BYTE MACRO",
	"DATA ENTRY TERMINAL", "SUPDUP", "SUPDUP OUTPUT",
	"SEND LOCATION", "TERMINAL TYPE", "END OF RECORD",
	"TACACS UID", "OUTPUT MARKING", "TTYLOC",
	"3270 REGIME", "X.3 PAD", "NAWS", "TSPEED", "LFLOW",
	"LINEMODE", "XDISPLOC", "OLD-ENVIRON", "AUTHENTICATION",
	"ENCRYPT", "NEW-ENVIRON",
	0,
};
#define	TELOPT_FIRST	TELOPT_BINARY
#define	TELOPT_LAST	TELOPT_NEW_ENVIRON
#define	TELOPT_OK(x)	((unsigned int)(x) <= TELOPT_LAST)
#define	TELOPT(x)	telopts[(x)-TELOPT_FIRST]
#endif

#define	TELQUAL_IS	0
#define	TELQUAL_SEND	1
#define	TELQUAL_INFO	2
#define	TELQUAL_REPLY	2
#define	TELQUAL_NAME	3

#define	LFLOW_OFF		0
#define	LFLOW_ON		1
#define	LFLOW_RESTART_ANY	2
#define	LFLOW_RESTART_XON	3


#define	LM_MODE		1
#define	LM_FORWARDMASK	2
#define	LM_SLC		3

#define	MODE_EDIT	0x01
#define	MODE_TRAPSIG	0x02
#define	MODE_ACK	0x04
#define MODE_SOFT_TAB	0x08
#define MODE_LIT_ECHO	0x10

#define	MODE_MASK	0x1f

#define MODE_FLOW		0x0100
#define MODE_ECHO		0x0200
#define MODE_INBIN		0x0400
#define MODE_OUTBIN		0x0800
#define MODE_FORCE		0x1000

#define	SLC_SYNCH	1
#define	SLC_BRK		2
#define	SLC_IP		3
#define	SLC_AO		4
#define	SLC_AYT		5
#define	SLC_EOR		6
#define	SLC_ABORT	7
#define	SLC_EOF		8
#define	SLC_SUSP	9
#define	SLC_EC		10
#define	SLC_EL		11
#define	SLC_EW		12
#define	SLC_RP		13
#define	SLC_LNEXT	14
#define	SLC_XON		15
#define	SLC_XOFF	16
#define	SLC_FORW1	17
#define	SLC_FORW2	18

#define	NSLC		18

#define	SLC_NAMELIST	"0", "SYNCH", "BRK", "IP", "AO", "AYT", "EOR", \
			"ABORT", "EOF", "SUSP", "EC", "EL", "EW", "RP", \
			"LNEXT", "XON", "XOFF", "FORW1", "FORW2", 0,
#ifdef	SLC_NAMES
char *slc_names[] = {
	SLC_NAMELIST
};
#else
extern char *slc_names[];
#define	SLC_NAMES SLC_NAMELIST
#endif

#define	SLC_NAME_OK(x)	((unsigned int)(x) <= NSLC)
#define SLC_NAME(x)	slc_names[x]

#define	SLC_NOSUPPORT	0
#define	SLC_CANTCHANGE	1
#define	SLC_VARIABLE	2
#define	SLC_DEFAULT	3
#define	SLC_LEVELBITS	0x03

#define	SLC_FUNC	0
#define	SLC_FLAGS	1
#define	SLC_VALUE	2

#define	SLC_ACK		0x80
#define	SLC_FLUSHIN	0x40
#define	SLC_FLUSHOUT	0x20

#define	OLD_ENV_VAR	1
#define	OLD_ENV_VALUE	0
#define	NEW_ENV_VAR	0
#define	NEW_ENV_VALUE	1
#define	ENV_ESC		2
#define ENV_USERVAR	3

#define	AUTH_WHO_CLIENT		0
#define	AUTH_WHO_SERVER		1
#define	AUTH_WHO_MASK		1

#define	AUTH_HOW_ONE_WAY	0
#define	AUTH_HOW_MUTUAL		2
#define	AUTH_HOW_MASK		2

#define	AUTHTYPE_NULL		0
#define	AUTHTYPE_KERBEROS_V4	1
#define	AUTHTYPE_KERBEROS_V5	2
#define	AUTHTYPE_SPX		3
#define	AUTHTYPE_MINK		4
#define	AUTHTYPE_CNT		5

#define	AUTHTYPE_TEST		99

#ifdef	AUTH_NAMES
char *authtype_names[] = {
	"NULL", "KERBEROS_V4", "KERBEROS_V5", "SPX", "MINK", 0,
};
#else
extern char *authtype_names[];
#endif

#define	AUTHTYPE_NAME_OK(x)	((unsigned int)(x) < AUTHTYPE_CNT)
#define	AUTHTYPE_NAME(x)	authtype_names[x]

#define	ENCRYPT_IS		0
#define	ENCRYPT_SUPPORT		1
#define	ENCRYPT_REPLY		2
#define	ENCRYPT_START		3
#define	ENCRYPT_END		4
#define	ENCRYPT_REQSTART	5
#define	ENCRYPT_REQEND		6
#define	ENCRYPT_ENC_KEYID	7
#define	ENCRYPT_DEC_KEYID	8
#define	ENCRYPT_CNT		9

#define	ENCTYPE_ANY		0
#define	ENCTYPE_DES_CFB64	1
#define	ENCTYPE_DES_OFB64	2
#define	ENCTYPE_CNT		3

#ifdef	ENCRYPT_NAMES
char *encrypt_names[] = {
	"IS", "SUPPORT", "REPLY", "START", "END",
	"REQUEST-START", "REQUEST-END", "ENC-KEYID", "DEC-KEYID",
	0,
};
char *enctype_names[] = {
	"ANY", "DES_CFB64",  "DES_OFB64",  0,
};
#else
extern char *encrypt_names[];
extern char *enctype_names[];
#endif


#define	ENCRYPT_NAME_OK(x)	((unsigned int)(x) < ENCRYPT_CNT)
#define	ENCRYPT_NAME(x)		encrypt_names[x]

#define	ENCTYPE_NAME_OK(x)	((unsigned int)(x) < ENCTYPE_CNT)
#define	ENCTYPE_NAME(x)		enctype_names[x]

#endif
