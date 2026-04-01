from .machine_init import run

if __name__ == "__main__":
    # Note(strtgbb): machine_init handles scaling on AWS infra,
    # but we have our own scaling mechanism, so don't run this
    return

    run()
