#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <termios.h>
#include <unistd.h>
#include <Server/ClientEmbedded/openpty.h>
#include <sys/ioctl.h>
#ifdef __FreeBSD__    // Not supported by FreeBSD version in sysroot https://www.gnu.org/software/gnulib/manual/html_node/ptsname_005fr.html
#include <cstdio>


static int ptsname_r(int fd, char * buf, size_t buflen)
{
    if (isatty(fd))
    {
        int pts_num;
        if (ioctl(fd, TIOCGPTN, &pts_num) == -1)
            return -1;

        int sz = snprintf(buf, buflen, "/dev/pts/%d", pts_num);
        return sz == 0;
    }

    return -1;
}
#endif


int openpty(int * amaster, int * aslave, char * name, const struct termios * termp, const struct winsize * winp)
{
    int master, slave;
    char slave_name[256];

    master = posix_openpt(O_RDWR | O_NOCTTY);
    if (master < 0)
    {
        return -1;
    }

    if (grantpt(master) < 0 || unlockpt(master) < 0)
    {
        close(master);
        return -1;
    }

    if (ptsname_r(master, slave_name, sizeof(slave_name)) != 0)
    {
        close(master);
        return -1;
    }

    if (name)
    {
        strncpy(name, slave_name, strlen(slave_name) + 1);
    }

    slave = open(slave_name, O_RDWR | O_NOCTTY);
    if (slave < 0)
    {
        close(master);
        return -1;
    }

    if (amaster)
    {
        *amaster = master;
    }
    if (aslave)
    {
        *aslave = slave;
    }

    if (termp)
    {
        tcsetattr(slave, TCSANOW, termp);
    }
    if (winp)
    {
        ioctl(slave, TIOCSWINSZ, winp);
    }

    return 0;
}
