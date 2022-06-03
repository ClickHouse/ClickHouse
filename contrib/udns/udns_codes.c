/* Automatically generated. */
#include "udns.h"

const struct dns_nameval dns_typetab[] = {
 {DNS_T_INVALID,"INVALID"},
 {DNS_T_A,"A"},
 {DNS_T_NS,"NS"},
 {DNS_T_MD,"MD"},
 {DNS_T_MF,"MF"},
 {DNS_T_CNAME,"CNAME"},
 {DNS_T_SOA,"SOA"},
 {DNS_T_MB,"MB"},
 {DNS_T_MG,"MG"},
 {DNS_T_MR,"MR"},
 {DNS_T_NULL,"NULL"},
 {DNS_T_WKS,"WKS"},
 {DNS_T_PTR,"PTR"},
 {DNS_T_HINFO,"HINFO"},
 {DNS_T_MINFO,"MINFO"},
 {DNS_T_MX,"MX"},
 {DNS_T_TXT,"TXT"},
 {DNS_T_RP,"RP"},
 {DNS_T_AFSDB,"AFSDB"},
 {DNS_T_X25,"X25"},
 {DNS_T_ISDN,"ISDN"},
 {DNS_T_RT,"RT"},
 {DNS_T_NSAP,"NSAP"},
 {DNS_T_NSAP_PTR,"NSAP_PTR"},
 {DNS_T_SIG,"SIG"},
 {DNS_T_KEY,"KEY"},
 {DNS_T_PX,"PX"},
 {DNS_T_GPOS,"GPOS"},
 {DNS_T_AAAA,"AAAA"},
 {DNS_T_LOC,"LOC"},
 {DNS_T_NXT,"NXT"},
 {DNS_T_EID,"EID"},
 {DNS_T_NIMLOC,"NIMLOC"},
 {DNS_T_SRV,"SRV"},
 {DNS_T_ATMA,"ATMA"},
 {DNS_T_NAPTR,"NAPTR"},
 {DNS_T_KX,"KX"},
 {DNS_T_CERT,"CERT"},
 {DNS_T_A6,"A6"},
 {DNS_T_DNAME,"DNAME"},
 {DNS_T_SINK,"SINK"},
 {DNS_T_OPT,"OPT"},
 {DNS_T_DS,"DS"},
 {DNS_T_SSHFP,"SSHFP"},
 {DNS_T_IPSECKEY,"IPSECKEY"},
 {DNS_T_RRSIG,"RRSIG"},
 {DNS_T_NSEC,"NSEC"},
 {DNS_T_DNSKEY,"DNSKEY"},
 {DNS_T_DHCID,"DHCID"},
 {DNS_T_NSEC3,"NSEC3"},
 {DNS_T_NSEC3PARAMS,"NSEC3PARAMS"},
 {DNS_T_TALINK,"TALINK"},
 {DNS_T_SPF,"SPF"},
 {DNS_T_UINFO,"UINFO"},
 {DNS_T_UID,"UID"},
 {DNS_T_GID,"GID"},
 {DNS_T_UNSPEC,"UNSPEC"},
 {DNS_T_TSIG,"TSIG"},
 {DNS_T_IXFR,"IXFR"},
 {DNS_T_AXFR,"AXFR"},
 {DNS_T_MAILB,"MAILB"},
 {DNS_T_MAILA,"MAILA"},
 {DNS_T_ANY,"ANY"},
 {DNS_T_ZXFR,"ZXFR"},
 {DNS_T_DLV,"DLV"},
 {DNS_T_MAX,"MAX"},
 {0,0}};
const char *dns_typename(enum dns_type code) {
 static char nm[20];
 switch(code) {
 case DNS_T_INVALID: return dns_typetab[0].name;
 case DNS_T_A: return dns_typetab[1].name;
 case DNS_T_NS: return dns_typetab[2].name;
 case DNS_T_MD: return dns_typetab[3].name;
 case DNS_T_MF: return dns_typetab[4].name;
 case DNS_T_CNAME: return dns_typetab[5].name;
 case DNS_T_SOA: return dns_typetab[6].name;
 case DNS_T_MB: return dns_typetab[7].name;
 case DNS_T_MG: return dns_typetab[8].name;
 case DNS_T_MR: return dns_typetab[9].name;
 case DNS_T_NULL: return dns_typetab[10].name;
 case DNS_T_WKS: return dns_typetab[11].name;
 case DNS_T_PTR: return dns_typetab[12].name;
 case DNS_T_HINFO: return dns_typetab[13].name;
 case DNS_T_MINFO: return dns_typetab[14].name;
 case DNS_T_MX: return dns_typetab[15].name;
 case DNS_T_TXT: return dns_typetab[16].name;
 case DNS_T_RP: return dns_typetab[17].name;
 case DNS_T_AFSDB: return dns_typetab[18].name;
 case DNS_T_X25: return dns_typetab[19].name;
 case DNS_T_ISDN: return dns_typetab[20].name;
 case DNS_T_RT: return dns_typetab[21].name;
 case DNS_T_NSAP: return dns_typetab[22].name;
 case DNS_T_NSAP_PTR: return dns_typetab[23].name;
 case DNS_T_SIG: return dns_typetab[24].name;
 case DNS_T_KEY: return dns_typetab[25].name;
 case DNS_T_PX: return dns_typetab[26].name;
 case DNS_T_GPOS: return dns_typetab[27].name;
 case DNS_T_AAAA: return dns_typetab[28].name;
 case DNS_T_LOC: return dns_typetab[29].name;
 case DNS_T_NXT: return dns_typetab[30].name;
 case DNS_T_EID: return dns_typetab[31].name;
 case DNS_T_NIMLOC: return dns_typetab[32].name;
 case DNS_T_SRV: return dns_typetab[33].name;
 case DNS_T_ATMA: return dns_typetab[34].name;
 case DNS_T_NAPTR: return dns_typetab[35].name;
 case DNS_T_KX: return dns_typetab[36].name;
 case DNS_T_CERT: return dns_typetab[37].name;
 case DNS_T_A6: return dns_typetab[38].name;
 case DNS_T_DNAME: return dns_typetab[39].name;
 case DNS_T_SINK: return dns_typetab[40].name;
 case DNS_T_OPT: return dns_typetab[41].name;
 case DNS_T_DS: return dns_typetab[42].name;
 case DNS_T_SSHFP: return dns_typetab[43].name;
 case DNS_T_IPSECKEY: return dns_typetab[44].name;
 case DNS_T_RRSIG: return dns_typetab[45].name;
 case DNS_T_NSEC: return dns_typetab[46].name;
 case DNS_T_DNSKEY: return dns_typetab[47].name;
 case DNS_T_DHCID: return dns_typetab[48].name;
 case DNS_T_NSEC3: return dns_typetab[49].name;
 case DNS_T_NSEC3PARAMS: return dns_typetab[50].name;
 case DNS_T_TALINK: return dns_typetab[51].name;
 case DNS_T_SPF: return dns_typetab[52].name;
 case DNS_T_UINFO: return dns_typetab[53].name;
 case DNS_T_UID: return dns_typetab[54].name;
 case DNS_T_GID: return dns_typetab[55].name;
 case DNS_T_UNSPEC: return dns_typetab[56].name;
 case DNS_T_TSIG: return dns_typetab[57].name;
 case DNS_T_IXFR: return dns_typetab[58].name;
 case DNS_T_AXFR: return dns_typetab[59].name;
 case DNS_T_MAILB: return dns_typetab[60].name;
 case DNS_T_MAILA: return dns_typetab[61].name;
 case DNS_T_ANY: return dns_typetab[62].name;
 case DNS_T_ZXFR: return dns_typetab[63].name;
 case DNS_T_DLV: return dns_typetab[64].name;
 case DNS_T_MAX: return dns_typetab[65].name;
 }
 return _dns_format_code(nm,"type",code);
}

const struct dns_nameval dns_classtab[] = {
 {DNS_C_INVALID,"INVALID"},
 {DNS_C_IN,"IN"},
 {DNS_C_CH,"CH"},
 {DNS_C_HS,"HS"},
 {DNS_C_ANY,"ANY"},
 {0,0}};
const char *dns_classname(enum dns_class code) {
 static char nm[20];
 switch(code) {
 case DNS_C_INVALID: return dns_classtab[0].name;
 case DNS_C_IN: return dns_classtab[1].name;
 case DNS_C_CH: return dns_classtab[2].name;
 case DNS_C_HS: return dns_classtab[3].name;
 case DNS_C_ANY: return dns_classtab[4].name;
 }
 return _dns_format_code(nm,"class",code);
}

const struct dns_nameval dns_rcodetab[] = {
 {DNS_R_NOERROR,"NOERROR"},
 {DNS_R_FORMERR,"FORMERR"},
 {DNS_R_SERVFAIL,"SERVFAIL"},
 {DNS_R_NXDOMAIN,"NXDOMAIN"},
 {DNS_R_NOTIMPL,"NOTIMPL"},
 {DNS_R_REFUSED,"REFUSED"},
 {DNS_R_YXDOMAIN,"YXDOMAIN"},
 {DNS_R_YXRRSET,"YXRRSET"},
 {DNS_R_NXRRSET,"NXRRSET"},
 {DNS_R_NOTAUTH,"NOTAUTH"},
 {DNS_R_NOTZONE,"NOTZONE"},
 {DNS_R_BADSIG,"BADSIG"},
 {DNS_R_BADKEY,"BADKEY"},
 {DNS_R_BADTIME,"BADTIME"},
 {0,0}};
const char *dns_rcodename(enum dns_rcode code) {
 static char nm[20];
 switch(code) {
 case DNS_R_NOERROR: return dns_rcodetab[0].name;
 case DNS_R_FORMERR: return dns_rcodetab[1].name;
 case DNS_R_SERVFAIL: return dns_rcodetab[2].name;
 case DNS_R_NXDOMAIN: return dns_rcodetab[3].name;
 case DNS_R_NOTIMPL: return dns_rcodetab[4].name;
 case DNS_R_REFUSED: return dns_rcodetab[5].name;
 case DNS_R_YXDOMAIN: return dns_rcodetab[6].name;
 case DNS_R_YXRRSET: return dns_rcodetab[7].name;
 case DNS_R_NXRRSET: return dns_rcodetab[8].name;
 case DNS_R_NOTAUTH: return dns_rcodetab[9].name;
 case DNS_R_NOTZONE: return dns_rcodetab[10].name;
 case DNS_R_BADSIG: return dns_rcodetab[11].name;
 case DNS_R_BADKEY: return dns_rcodetab[12].name;
 case DNS_R_BADTIME: return dns_rcodetab[13].name;
 }
 return _dns_format_code(nm,"rcode",code);
}
