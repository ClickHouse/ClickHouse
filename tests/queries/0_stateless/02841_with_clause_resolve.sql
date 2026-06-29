set enable_analyzer = 1;

WITH
    -- Input
    44100 AS sample_frequency
    , number AS tick
    , tick / sample_frequency AS time

    -- Delay
    , (time, wave, delay_, decay, count) -> arraySum(n1 -> wave(time - delay_ * n1), range(count)) AS delay

    , delay(time, (time -> 0.5), 0.2, 0.5, 5) AS kick

SELECT

    kick

FROM system.numbers
LIMIT 5;

WITH
    -- Input
    44100 AS sample_frequency
    , number AS tick
    , tick / sample_frequency AS time

    -- Output control
    , 1 AS master_volume
    , level -> least(1.0, greatest(-1.0, level)) AS clamp
    , level -> (clamp(level) * 0x7FFF * master_volume)::Int16 AS output
    , x -> (x, x) AS mono

    -- Basic waves
    , time -> sin(time * 2 * pi()) AS sine_wave
    , time -> time::UInt64 % 2 * 2 - 1 AS square_wave
    , time -> (time - floor(time)) * 2 - 1 AS sawtooth_wave
    , time -> abs(sawtooth_wave(time)) * 2 - 1 AS triangle_wave

    -- Helpers
    , (from, to, wave, time) -> from + ((wave(time) + 1) / 2) * (to - from) AS lfo
    , (from, to, steps, time) -> from + floor((time - floor(time)) * steps) / steps * (to - from) AS step_lfo
    , (from, to, steps, time) -> exp(step_lfo(log(from), log(to), steps, time)) AS exp_step_lfo

    -- Noise
    , time -> cityHash64(time) / 0xFFFFFFFFFFFFFFFF AS uniform_noise
    , time -> erf(uniform_noise(time)) AS white_noise
    , time -> cityHash64(time) % 2 ? 1 : -1 AS bernoulli_noise

    -- Distortion
    , (x, amount) -> clamp(x * amount) AS clipping
    , (x, amount) -> clamp(x > 0 ? pow(x, amount) : -pow(-x, amount)) AS power_distortion
    , (x, amount) -> round(x * exp2(amount)) / exp2(amount) AS bitcrush
    , (time, sample_frequency) -> round(time * sample_frequency) / sample_frequency AS desample
    , (time, wave, amount) -> (time - floor(time) < (1 - amount)) ? wave(time * (1 - amount)) : 0 AS thin
    , (time, wave, amount) -> wave(floor(time) + pow(time - floor(time), amount)) AS skew

    -- Combining
    , (a, b, weight) -> a * (1 - weight) + b * weight AS combine

    -- Envelopes
    , (time, offset, attack, hold, release) ->
        time < offset ? 0
        : (time < offset + attack                  ? ((time - offset) / attack)
        : (time < offset + attack + hold           ? 1
        : (time < offset + attack + hold + release ? (offset + attack + hold + release - time) / release
        : 0))) AS envelope

    , (bpm, time, offset, attack, hold, release) ->
        envelope(
            time * (bpm / 60) - floor(time * (bpm / 60)),
            offset,
            attack,
            hold,
            release) AS running_envelope

    -- Sequencers
    , (sequence, time) -> sequence[1 + time::UInt64 % length(sequence)] AS sequencer

    -- Delay
    , (time, wave, delay, decay, count) -> arraySum(n -> wave(time - delay * n) * pow(decay, n), range(count)) AS delay


    , delay(time, (time -> power_distortion(sine_wave(time * 80 + sine_wave(time * 2)), lfo(0.5, 1, sine_wave, time / 16))
            * running_envelope(60, time, 0, 0.0, 0.01, 0.1)),
            0.2, 0.5, 5) AS kick

SELECT

    (output(
        kick +
        delay(time, (time ->
            power_distortion(
                sine_wave(time * 50 + 1 * sine_wave(time * 100 + 1/4))
                    * running_envelope(60, time, 0, 0.01, 0.01, 0.1),
                lfo(1, 0.75, triangle_wave, time / 8))),
            0.2, 0.5, 10)
        * lfo(0.5, 1, triangle_wave, time / 7)

        + delay(time, (time ->
            power_distortion(
                sine_wave(time * sequencer([50, 100, 200, 400], time / 2) + 1 * sine_wave(time * sequencer([50, 100, 200], time / 4) + 1/4))
                    * running_envelope(60, time, 0.5, 0.01, 0.01, 0.1),
                lfo(1, 0.75, triangle_wave, time / 8))),
            0.2, 0.5, 10)
        * lfo(0.5, 1, triangle_wave, 16 + time / 11)

        + delay(time, (time ->
            white_noise(time) * running_envelope(60, time, 0.75, 0.01, 0.01, 0.1)),
            0.2, 0.5, 10)
        * lfo(0.5, 1, triangle_wave, 24 + time / 13)

        + sine_wave(time * 100 + 1 * sine_wave(time * 10 + 1/4))
            * running_envelope(120, time, 0, 0.01, 0.01, 0.1)
    ),

    output(
        kick +
        delay(time + 0.01, (time ->
            power_distortion(
                sine_wave(time * 50 + 1 * sine_wave(time * 100 + 1/4))
                    * running_envelope(60, time, 0, 0.01, 0.01, 0.1),
                lfo(1, 0.75, triangle_wave, time / 8))),
            0.2, 0.5, 10)
        * lfo(0.5, 1, triangle_wave, time / 7)

        + delay(time - 0.01, (time ->
            power_distortion(
                sine_wave(time * sequencer([50, 100, 200, 400], time / 2) + 1 * sine_wave(time * sequencer([50, 100, 200], time / 4) + 1/4))
                    * running_envelope(60, time, 0.5, 0.01, 0.01, 0.1),
                lfo(1, 0.75, triangle_wave, time / 8))),
            0.2, 0.5, 10)
        * lfo(0.5, 1, triangle_wave, 16 + time / 11)

        + delay(time + 0.005, (time ->
            white_noise(time) * running_envelope(60, time, 0.75, 0.01, 0.01, 0.1)),
            0.2, 0.5, 10)
        * lfo(0.5, 1, triangle_wave, 24 + time / 13)
    ))

FROM system.numbers
LIMIT 10;
