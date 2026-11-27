# For a thread whose signal interrupted the sanitizer runtime, print the registers of the
# interrupted frame and the memory they point to: its backtrace does not show which pointer was bad.

# Prints 6 words at the address in $arg0 if that memory is readable. The value is taken in the
# caller's frame, since `frame apply` evaluates `x` in the innermost one.
define sanitizer_fault_memory
    set $sanitizer_fault_address = $arg0
    frame apply 1 -s -q x/6gx $sanitizer_fault_address
end

define sanitizer_fault
    # The shell exits with 100 + the level of the frame a signal interrupted inside the sanitizer
    # runtime (TSan registers `sighandler` with the kernel), and below 100 if there is none or awk fails.
    pipe bt | awk '/^#[0-9]+ +(0x[0-9a-f]+ in )?sighandler ?\(/ { s = 1; next } s == 1 && /^#[0-9]+ +<signal handler called>/ { s = 2; next } s == 2 && /^#[0-9]+ +(0x[0-9a-f]+ in )?__(tsan|sanitizer)::/ { l = substr($1, 2) + 0; if (!r && l <= 155) r = 100 + l } { s = 0 } END { exit r }'
    if $_shell_exitcode >= 100
        set $sanitizer_fault_level = $_shell_exitcode - 100
        select-frame level $sanitizer_fault_level
        echo \nThread interrupted by a signal inside the sanitizer runtime:\n
        thread
        frame
        x/i $pc
        info registers
        sanitizer_fault_memory $rax
        sanitizer_fault_memory $rbx
        sanitizer_fault_memory $rcx
        sanitizer_fault_memory $rdx
        sanitizer_fault_memory $rsi
        sanitizer_fault_memory $rdi
        sanitizer_fault_memory $rbp
        sanitizer_fault_memory $rsp
        sanitizer_fault_memory $r8
        sanitizer_fault_memory $r9
        sanitizer_fault_memory $r10
        sanitizer_fault_memory $r11
        sanitizer_fault_memory $r12
        sanitizer_fault_memory $r13
        sanitizer_fault_memory $r14
        sanitizer_fault_memory $r15
    end
end

thread apply all -s -q sanitizer_fault
