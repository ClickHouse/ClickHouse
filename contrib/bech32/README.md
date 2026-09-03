Code copied from the [bech32](https://github.com/sipa/bech32) repository. It contains reference implementations 
of the Bech32 and Bech32m algorithms, along with an implementation of the SegWit algorithm which is a 
specialization of Bech32(m) for Bitcoin. The code used in ClickHouse is only the Bech32 algorithms, not SegWit. 
See [this link](https://en.bitcoin.it/wiki/Bech32) for more info on the algorithms and their applications.

Modifications to the code:
- typedef changed to using as per compiler warnings
- Problematic asserts were changed to if statement + empty return
- convertbits function was copied directly into src/Functions/FunctionsBech32Representation.cpp
