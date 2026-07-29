#pragma once

#include <optional>

#include <Core/Block.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>

namespace DB
{

/* A TableJoin-free build/probe execution layer, generalized from the hash-join wiring
 * (`FillingRightJoinSideTransform` / `JoiningTransform` / `joinPipelinesRightLeft`).
 *
 * The build side ends in a `JoinBuildSideTransform` whose only output carries no data
 * (empty header); `QueryPipelineBuilder::joinPipelinesBuildProbe` fans that port out to a
 * barrier input on one `JoinProbeSideTransform` per probe stream. Each probe keeps its data
 * input not-needed until the barrier input finishes, so the probe side is held back by
 * backpressure, nothing is buffered. The build transform publishes its finalized state
 * (a typed shared_ptr slot created at wiring time) before finishing the port, and probes
 * read it only after observing the closed barrier — the port closure is the happens-before
 * edge. No join-object interface is involved; per-stream probe state lives in the concrete
 * probe transform.
 */

/// Consumes the whole (single-stream) build side, then finalizes and publishes the shared
/// state and closes the barrier port.
class JoinBuildSideTransform : public IProcessor
{
public:
    explicit JoinBuildSideTransform(SharedHeader input_header);

    Status prepare() override;
    void work() override;

protected:
    /// Returns false to stop reading the input early (size-limits 'break' overflow mode);
    /// finishBuild is still called afterwards.
    virtual bool consumeBuildChunk(Chunk chunk) = 0;

    /// Called exactly once, after the last consumed chunk. Must leave the shared state
    /// fully published: the barrier output finishes right after and releases the probes.
    virtual void finishBuild() = 0;

private:
    Chunk input_chunk;
    bool has_input = false;
    bool stop_reading = false;
    bool build_finished = false;
};

/// Joins one probe-side stream against the state published by the build transform.
/// Input 0 carries the probe data, input 1 is the data-free barrier port.
class JoinProbeSideTransform : public IProcessor
{
public:
    JoinProbeSideTransform(SharedHeader input_header, SharedHeader output_header);

    Status prepare() override;
    void work() override;

protected:
    /// Called exactly once, when the barrier input has finished: the build state is
    /// published and safe to grab.
    virtual void onBarrierReleased() = 0;

    /// Accepts the next probe chunk; produceChunk is then called until it returns nullopt.
    virtual void consumeProbeChunk(Chunk chunk) = 0;

    /// The next output chunk for the last consumed chunk, or nullopt when it is exhausted
    /// and the transform needs more input. Called once per scheduling round, so a probe
    /// producing many chunks observes cancellation between them. A returned chunk with no
    /// rows and no chunk infos is a pure yield: nothing is pushed and the call repeats on
    /// the next round (for work-budget pacing of long low-output scans).
    virtual std::optional<Chunk> produceChunk() = 0;

private:
    Chunk input_chunk;
    std::optional<Chunk> output_chunk;
    bool has_input = false;
    bool producing = false;
    bool barrier_released = false;
};

}
