#ifndef _SYS_TTYDEFAULTS_H
#define _SYS_TTYDEFAULTS_H

#define TTYDEF_IFLAG (BRKINT | ISTRIP | ICRNL | IMAXBEL | IXON | IXANY)
#define TTYDEF_OFLAG (OPOST | ONLCR | XTABS)
#define TTYDEF_LFLAG (ECHO | ICANON | ISIG | IEXTEN | ECHOE|ECHOKE|ECHOCTL)
#define TTYDEF_CFLAG (CREAD | CS7 | PARENB | HUPCL)
#define TTYDEF_SPEED (B9600)
#define CTRL(x) ((x)&037)
#define CEOF CTRL('d')

#define CEOL '\0'
#define CSTATUS '\0'

#define CERASE 0177
#define CINTR CTRL('c')
#define CKILL CTRL('u')
#define CMIN 1
#define CQUIT 034
#define CSUSP CTRL('z')
#define CTIME 0
#define CDSUSP CTRL('y')
#define CSTART CTRL('q')
#define CSTOP CTRL('s')
#define CLNEXT CTRL('v')
#define CDISCARD CTRL('o')
#define CWERASE CTRL('w')
#define CREPRINT CTRL('r')
#define CEOT CEOF
#define CBRK CEOL
#define CRPRNT CREPRINT
#define CFLUSH CDISCARD

#endif
