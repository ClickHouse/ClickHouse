/*
 * Minimal stub for the libpcap BPF filter compiler.
 *
 * ClickHouse uses libpcap only to READ capture files (pcap / pcapng); it never
 * compiles BPF filter expressions. The real implementation lives in gencode.c
 * and depends on the bison-generated grammar.c, whose GPL-licensed output we do
 * not ship. gencode.c and the grammar/scanner are therefore excluded from the
 * build (see CMakeLists.txt), and these stubs stand in for the few public
 * symbols so the archive links cleanly. Any accidental call fails gracefully.
 */

#include <config.h>

#include <pcap/pcap.h>
#include "pcap-int.h"

#include <stdio.h>
#include <stdlib.h>

int
pcap_compile(pcap_t *p, struct bpf_program *program,
	     const char *buf, int optimize, bpf_u_int32 mask)
{
	(void)program;
	(void)buf;
	(void)optimize;
	(void)mask;
	(void)snprintf(p->errbuf, PCAP_ERRBUF_SIZE,
	    "BPF filter compilation is not supported in this build");
	return (PCAP_ERROR);
}

int
pcap_compile_nopcap(int snaplen_arg, int linktype_arg,
		    struct bpf_program *program,
		    const char *buf, int optimize, bpf_u_int32 mask)
{
	(void)snaplen_arg;
	(void)linktype_arg;
	(void)program;
	(void)buf;
	(void)optimize;
	(void)mask;
	return (PCAP_ERROR);
}

/*
 * Not part of the compiler: frees a (possibly loaded) filter program. The read
 * path (savefile.c, pcap.c, optimize.c) needs this, and it is independent of
 * the grammar, so we provide the genuine implementation here. Kept identical to
 * upstream gencode.c.
 */
void
pcap_freecode(struct bpf_program *program)
{
	program->bf_len = 0;
	if (program->bf_insns != NULL)
	{
		free((char *)program->bf_insns);
		program->bf_insns = NULL;
	}
}
