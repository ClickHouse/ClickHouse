from praktika._environment import _Environment


# TODO: find better place and/or right storage for parameter
def get_param():
    env = _Environment.get()
    assert env.PARAMETER
    return env.PARAMETER
