#define FE_ALL_EXCEPT 0
#define FE_TONEAREST  0

typedef unsigned long fexcept_t;

typedef struct {
	unsigned long __cw;
} fenv_t;

#define FE_DFL_ENV      ((const fenv_t *) -1)
