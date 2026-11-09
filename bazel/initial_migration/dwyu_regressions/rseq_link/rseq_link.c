extern int sched_getcpu(void);
extern float expf(float);

int main(int argc, char **argv)
{
    int (*volatile current_cpu)(void) = sched_getcpu;
    float (*volatile compute_exp)(float) = expf;
    (void)argv;
    return current_cpu() < 0 || compute_exp((float)argc) <= 0.0f;
}
