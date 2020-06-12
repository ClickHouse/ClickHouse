/*
 * Public domain
 *
 * Dongsheng Song <dongsheng.song@gmail.com>
 * Brent Cook <bcook@openbsd.org>
 */

#include <ws2tcpip.h>

#include <openssl/bio.h>
#include <openssl/err.h>

int
BIO_sock_init(void)
{
	/*
	 * WSAStartup loads the winsock .dll and initializes the networking
	 * stack on Windows, or simply increases the reference count.
	 */
	static struct WSAData wsa_state = {0};
	WORD version_requested = MAKEWORD(2, 2);
	static int wsa_init_done = 0;
	if (!wsa_init_done) {
		if (WSAStartup(version_requested, &wsa_state) != 0) {
			int err = WSAGetLastError();
			SYSerror(err);
			BIOerror(BIO_R_WSASTARTUP);
			return (-1);
		}
		wsa_init_done = 1;
	}
 	return (1);
}

void
BIO_sock_cleanup(void)
{
	/*
	 * We could call WSACleanup here, but it is easy to get it wrong. Since
	 * this API provides no way to even tell if it failed, there is no safe
	 * way to expose that functionality here.
	 *
	 * The cost of leaving the networking DLLs loaded may have been large
	 * during the Windows 3.1/win32s era, but it is small in modern
	 * contexts, so don't bother.
	 */
}

int
BIO_socket_nbio(int s, int mode)
{
	u_long value = mode;
	return ioctlsocket(s, FIONBIO, &value) != SOCKET_ERROR;
}
