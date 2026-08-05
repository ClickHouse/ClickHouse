import { galaxyOnClick } from '/snippets/lib/galaxy.jsx';

export const BetaBadge = ({ link, galaxyTrack, galaxyEvent }) => {
    if (link) {
        return (
            <a
                href={link}
                target="_blank"
                rel="noopener noreferrer"
                className="betaBadge"
                onClick={galaxyTrack && galaxyEvent ? galaxyOnClick(galaxyEvent) : undefined}
            >
                <span>Beta</span>
            </a>
        )
    }

    return (
        <a
            href="/zh/reference/settings/beta-and-experimental-features#beta-features"
            className="betaBadge"
        >
            <span>Beta 版功能</span>
        </a>
    )
}

export default BetaBadge;
