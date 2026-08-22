/*
 * Minimal stubs for the libpcap BPF filter compiler and filter engine.
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

/*
 * The BPF filter engine (bpf_filter.c, together with bpf_image.c and bpf_dump.c)
 * is excluded from the build as well: on Linux it includes <linux/filter.h>,
 * which is missing from some cross-compilation sysroots, and it is reachable
 * only through a compiled filter program, which this build cannot produce
 * anyway - pcap_compile above always fails.
 *
 * pcapint_validate_filter therefore rejects every program, so pcap_setfilter
 * fails with "BPF program is not valid" instead of silently dropping packets,
 * and pcapint_filter - which the read path calls only after a filter has been
 * installed - is unreachable.
 */
int
pcapint_validate_filter(const struct bpf_insn *f, int len)
{
	(void)f;
	(void)len;
	return (0);
}

u_int
pcapint_filter(const struct bpf_insn *pc, const u_char *p, u_int wirelen,
	       u_int buflen)
{
	(void)pc;
	(void)p;
	(void)wirelen;
	(void)buflen;
	abort();
}
