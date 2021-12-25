#ifndef _SYS_REBOOT_H
#define _SYS_REBOOT_H
#ifdef __cplusplus
extern "C" {
#endif

#define RB_AUTOBOOT     0x01234567
#define RB_HALT_SYSTEM  0xcdef0123
#define RB_ENABLE_CAD   0x89abcdef
#define RB_DISABLE_CAD  0
#define RB_POWER_OFF    0x4321fedc
#define RB_SW_SUSPEND   0xd000fce2
#define RB_KEXEC        0x45584543

int reboot(int);

#ifdef __cplusplus
}
#endif
#endif
