/**
 * ReleaseSchedule — table showing the rollout schedule for ClickHouse Cloud
 * versions across Fast / Regular / Slow channels. Mintlify shim of
 * clickhouse-docs's `src/components/ReleaseSchedule`.
 *
 * Usage:
 *   import ReleaseSchedule from "/snippets/components/ReleaseSchedule/ReleaseSchedule.jsx";
 *
 *   <ReleaseSchedule releases={[
 *     { version: "25.4", fast_start_date: "...", ... fast_progress: "green", ... },
 *     ...
 *   ]} />
 */

const ReleaseSchedule = ({ releases = [] }) => {
  const groupStartStyle = {
    borderLeft: "1px solid rgba(128, 128, 128, 0.35)",
    paddingLeft: 16,
  };

  const StatusIndicator = ({ status }) => {
    const color =
      status === "green" ? "#22c55e" :
      status === "orange" ? "#f59e0b" :
      "#ef4444";
    return (
      <span style={{
        display: "inline-block",
        width: 8,
        height: 8,
        borderRadius: "50%",
        background: color,
        marginRight: 6,
      }} />
    );
  };

  const DateCell = ({ date, note, status }) => (
    <span style={{ whiteSpace: "nowrap" }}>
      {status && <StatusIndicator status={status} />}
      {note ? <Tooltip tip={note}>{date}</Tooltip> : date}
    </span>
  );

  return (
    <table>
      <colgroup />
      <colgroup span={2} />
      <colgroup span={2} />
      <colgroup span={2} />
      <thead>
        <tr>
          <th rowSpan={2} scope="col">Version</th>
          <th colSpan={2} scope="colgroup" style={groupStartStyle}>
            <a href="/docs/manage/updates#fast-release-channel-early-upgrades">Fast Channel</a>
          </th>
          <th colSpan={2} scope="colgroup" style={groupStartStyle}>
            <a href="/docs/manage/updates#regular-release-channel">Regular Channel</a>
          </th>
          <th colSpan={2} scope="colgroup" style={groupStartStyle}>
            <a href="/docs/manage/updates#slow-release-channel-deferred-upgrades">Slow Channel</a>
          </th>
        </tr>
        <tr>
          <th scope="col" style={groupStartStyle}>Start</th>
          <th scope="col">End</th>
          <th scope="col" style={groupStartStyle}>Start</th>
          <th scope="col">End</th>
          <th scope="col" style={groupStartStyle}>Start</th>
          <th scope="col">End</th>
        </tr>
      </thead>
      <tbody>
        {releases.map((release, idx) => {
          const isCompleted = [
            release.fast_start_date,
            release.fast_end_date,
            release.regular_start_date,
            release.regular_end_date,
            release.slow_start_date,
            release.slow_end_date,
          ].every((date) => date === "Completed");

          return (
            <tr key={idx}>
              <th scope="row">
                {release.changelog_link ? (
                  <a href={release.changelog_link} target="_blank" rel="noopener noreferrer">
                    {release.version}
                  </a>
                ) : (
                  release.version
                )}
              </th>
              {isCompleted ? (
                <td colSpan={6} style={{ textAlign: "center" }}>
                  <DateCell date="Completed" status="green" />
                </td>
              ) : (
                <>
                  <td style={groupStartStyle}><DateCell date={release.fast_start_date} note={release.fast_delay_note} status={release.fast_progress} /></td>
                  <td><DateCell date={release.fast_end_date} status={release.fast_progress} /></td>
                  <td style={groupStartStyle}><DateCell date={release.regular_start_date} note={release.regular_delay_note} status={release.regular_progress} /></td>
                  <td><DateCell date={release.regular_end_date} status={release.regular_progress} /></td>
                  <td style={groupStartStyle}><DateCell date={release.slow_start_date} note={release.slow_delay_note} status={release.slow_progress} /></td>
                  <td><DateCell date={release.slow_end_date} status={release.slow_progress} /></td>
                </>
              )}
            </tr>
          );
        })}
      </tbody>
    </table>
  );
};

export default ReleaseSchedule;
