#include <stdio.h>
#include <btrie.h>

int main()
{
    btrie_t *it;
    int            ret;

    uint8_t prefix_v6[16] = {0xde, 0xad, 0xbe, 0xef};
    uint8_t mask_v6[16] = {0xff, 0xff, 0xff};
    uint8_t ip_v6[16] = {0xde, 0xad, 0xbe, 0xef, 0xde};

    it = btrie_create();
    if (it == NULL) {
        printf("create error!\n");
        return 0;
    }

    //add 101.45.69.50/16
    ret = btrie_insert(it, 1697465650, 0xffff0000, 1);
    if (ret != 0) {
        printf("insert 1 error.\n");
        goto error;
    }

    //add 10.45.69.50/16
    ret = btrie_insert(it, 170738994, 0xffff0000, 1);
    if (ret != 0) {
        printf("insert 2 error.\n");
        goto error;
    }

    //add 10.45.79.50/16
    ret = btrie_insert(it, 170741554, 0xffff0000, 1);
    if (ret == 0) {
        printf("insert 3 error.\n");
        goto error;
    }

    //add 102.45.79.50/24
    ret = btrie_insert(it, 1714245426, 0xffffff00, 1);
    if (ret != 0) {
        printf("insert 4 error.\n");
        goto error;
    }

    ret = btrie_find(it, 170741554);
    if (ret == 1) {
        printf("test case 1 passed\n");
    } else {
        printf("test case 1 error\n");
    }

    ret = btrie_find(it, 170786817);
    if (ret != 1) {
        printf("test case 2 passed\n");
    } else {
        printf("test case 2 error\n");
    }

    ret = btrie_delete(it, 1714245426, 0xffffff00);
    if (ret != 0) {
        printf("delete 1 error\n");
        goto error;
    }
    
    ret = btrie_find(it, 1714245426);
    if (ret != 1) {
        printf("test case 3 passed\n");
    } else {
        printf("test case 3 error\n");
    }

    //add dead:beef::/32
    ret = btrie_insert_a6(it, prefix_v6, mask_v6, 1);
    if (ret != 0) {
        printf("insert 5 error\n");
        goto error;
    }

    ret = btrie_find_a6(it, ip_v6);
    if (ret == 1) {
        printf("test case 4 passed\n");
    } else {
        printf("test case 4 error\n");
    }

    return 0;
    
 error:
    btrie_destroy(it);
    printf("test failed\n");
    return 1;
}
