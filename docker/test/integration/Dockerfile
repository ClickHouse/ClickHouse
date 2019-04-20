FROM ubuntu:18.04

RUN apt-get update && apt-get -y install tzdata python llvm-6.0 llvm-6.0-dev

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

CMD echo "TSAN_OPTIONS='halt_on_error=1 history_size=7'" >> /etc/environment;
CMD echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment;
CMD echo "ASAN_SYMBOLIZER_PATH=/usr/lib/llvm-6.0/bin/llvm-symbolizer" >> /etc/environment;
CMD echo "UBSAN_SYMBOLIZER_PATH=/usr/lib/llvm-6.0/bin/llvm-symbolizer" >> /etc/environment;
CMD echo "LLVM_SYMBOLIZER_PATH=/usr/lib/llvm-6.0/bin/llvm-symbolizer" >> /etc/environment;
